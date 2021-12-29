package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.event.block.Block
import com.rarible.blockchain.scanner.event.block.BlockStatus
import com.rarible.blockchain.scanner.event.block.toBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.StableBlockEvent
import com.rarible.blockchain.scanner.util.BlockRanges
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.lastOrNull
import kotlinx.coroutines.flow.map
import org.slf4j.LoggerFactory

/**
 * Synchronizes database with the blockchain, see [onNewBlock].
 */
class BlockHandler<BB : BlockchainBlock>(
    private val blockClient: BlockchainBlockClient<BB>,
    private val blockService: BlockService,
    private val blockEventListeners: List<BlockEventListener>,
    private val batchLoad: BlockBatchLoadProperties
) {

    private val logger = LoggerFactory.getLogger(BlockHandler::class.java)

    /**
     * Handle a new block event and synchronize the database state with blockchain.
     *
     * Implementation of this method is approximately as follows:
     * 1) Firstly, we find the latest known block that is synced with the blockchain (by comparing block hashes)
     * and revert all out-of-sync blocks in the reversed order (from the newest to the oldest).
     * 2) Secondly, we process blocks from the last known correct block to the newest block.
     * For stable enough blocks we use batch processing that is faster in some blockchain clients
     * (Ethereum API has batch log requests).
     *
     * Upon returning from this method the database is consistent with the blockchain up to some point,
     * not necessarily up to the [newBlockchainBlock]. It may happen that the chain has been reorganized
     * while this method was syncing forward. The next call of this method will sync the remaining blocks.
     */
    suspend fun onNewBlock(newBlockchainBlock: BB) {
        val newBlock = newBlockchainBlock.toBlock()
        logger.info("Received new Block [{}:{}]: {}", newBlock.id, newBlock.hash, newBlock)

        val lastKnownBlock = getLastKnownBlock()
        logger.info("Last known Block [{}:{}]: {}", lastKnownBlock.id, lastKnownBlock.hash, lastKnownBlock)

        // If new block's parent hash is the same as hash of last known block, we could omit blockchain reorg check
        if (newBlock.parentHash == lastKnownBlock.hash) {
            if (lastKnownBlock.status == BlockStatus.PENDING) {
                logger.info(
                    "Retrying to process the last known pending block [{}:{}]",
                    lastKnownBlock.id,
                    lastKnownBlock.hash
                )
                processBlock(lastKnownBlock, false)
            }
            logger.info(
                "New block [{}:{}] is consistent with the last known block by parent hash {}",
                newBlock.id,
                newBlock.hash,
                lastKnownBlock.hash
            )
            logger.info("Processing the new consistent block [{}:{}]", newBlock.id, newBlock.hash)
            processBlock(newBlock, false)
            return
        }

        restoreMissingBlocks(lastKnownBlock, newBlock)
    }

    private suspend fun restoreMissingBlocks(lastKnownBlock: Block, newBlock: Block) {
        var currentBlock = findLatestCorrectKnownBlockAndRevertOther(lastKnownBlock)

        currentBlock = if (batchLoad.enabled && newBlock.id - currentBlock.id > batchLoad.confirmationBlockDistance) {
            batchSyncMissingBlocks(
                lastKnownBlock = currentBlock,
                limit = newBlock.id - currentBlock.id - batchLoad.confirmationBlockDistance
            ) ?: currentBlock
        } else {
            currentBlock
        }
        if (currentBlock.status == BlockStatus.PENDING) {
            logger.info(
                "Processing the latest known pending block [{}:{}]",
                currentBlock.id,
                currentBlock.hash
            )
            currentBlock = processBlock(currentBlock, false)
        }
        while (currentBlock.id < newBlock.id) {
            val nextBlock = getNextBlock(currentBlock, newBlock.id - currentBlock.id)
            if (nextBlock == null) {
                logger.warn(
                    "We haven't found the next block for [{}:{}] up until ${newBlock.id}, " +
                            "apparently, a reorganization has happened and the block [{}:{}] has disappeared",
                    currentBlock.id,
                    currentBlock.hash,
                    newBlock.id,
                    newBlock.hash
                )
                return
            }
            if (currentBlock.hash != nextBlock.parentHash) {
                logger.info(
                    "Blockchain has been reorganized while syncing blocks: [{}:{}] is not a parent of [{}:{}] but expected {}",
                    currentBlock.id,
                    currentBlock.hash,
                    nextBlock.id,
                    nextBlock.hash,
                    nextBlock.parentHash
                )
                return
            }
            currentBlock = processBlock(nextBlock, false)
        }
    }

    /**
     * Checks if chain reorg happened. Find the latest correct block
     * and revert others in the reversed order (from the newest to the oldest).
     * Returns the latest correct known block.
     */
    private suspend fun findLatestCorrectKnownBlockAndRevertOther(lastKnownBlock: Block): Block {
        var currentBlock = lastKnownBlock
        var blockchainBlock = fetchBlock(currentBlock.id)

        while (blockchainBlock == null || currentBlock.hash != blockchainBlock.hash) {
            revertBlock(currentBlock)
            currentBlock = getPreviousBlock(currentBlock)
            blockchainBlock = fetchBlock(currentBlock.id)
        }

        return currentBlock
    }

    private suspend fun getLastKnownBlock(): Block {
        val lastKnownBlock = blockService.getLastBlock()
        if (lastKnownBlock != null) {
            return lastKnownBlock
        }

        logger.info("Fetching the block #0 because there is no last known block")
        val firstBlock = fetchBlock(0) ?: error("Root block #0 is not found")

        return processBlock(firstBlock, false)
    }

    private suspend fun getNextBlock(startBlock: Block, maxSteps: Long): Block? {
        var id = startBlock.id
        val maxId = startBlock.id + maxSteps
        while (id < maxId) {
            val block = fetchBlock(id + 1)
            if (block != null) return block
            id++
        }
        return null
    }

    private suspend fun getPreviousBlock(startBlock: Block): Block {
        var id = startBlock.id
        while (id > 0) {
            val block = blockService.getBlock(id - 1)
            if (block != null) return block
            id--
        }
        error("Can't find previous block for: $startBlock")
    }

    private suspend fun batchSyncMissingBlocks(lastKnownBlock: Block, limit: Long): Block? = coroutineScope {
        BlockRanges.getRanges(
            from = lastKnownBlock.id + 1,
            to = lastKnownBlock.id + limit,
            step = batchLoad.batchSize
        )
            .map { range ->
                logger.info("Fetching blockchain blocks $range")
                range.map { id -> async { fetchBlock(id) } }.awaitAll()
            }
            .map { it.filterNotNull() }
            .filter { it.isNotEmpty() }
            .map {
                logger.info("Processing batch of ${it.size} stable blocks: ${it.first().id}..${it.last().id}")
                processBlocks(it, true)
            }
            .lastOrNull()
            ?.lastOrNull()
    }

    private suspend fun processBlocks(blocks: List<Block>, stable: Boolean): List<Block> {
        // TODO: implement this function for the whole batch.
        //  Currently, we can have only one PENDING block at the same time, so we have to process them one by one.
        return blocks.map { processBlock(it, stable) }
    }

    private suspend fun processBlock(block: Block, stable: Boolean): Block {
        blockService.save(block.copy(status = BlockStatus.PENDING))
        val event = if (stable) {
            StableBlockEvent(number = block.id, hash = block.hash)
        } else {
            NewBlockEvent(number = block.id, hash = block.hash)
        }
        processBlockEvents(listOf(event))
        return blockService.save(block.copy(status = BlockStatus.SUCCESS))
    }

    private suspend fun revertBlock(block: Block) {
        logger.info("Reverting block [{}:{}]: {}", block.id, block.hash, block)
        processBlockEvents(listOf(RevertedBlockEvent(number = block.id, hash = block.hash)))
        blockService.remove(block.id)
    }

    private suspend fun processBlockEvents(blockEvents: List<BlockEvent>) {
        blockEventListeners.forEach { it.process(blockEvents) }
    }

    private suspend fun fetchBlock(number: Long): Block? =
        blockClient.getBlock(number)?.toBlock()
}
