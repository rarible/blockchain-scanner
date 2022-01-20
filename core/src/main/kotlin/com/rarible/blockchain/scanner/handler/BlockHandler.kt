package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.block.BlockStatus
import com.rarible.blockchain.scanner.block.toBlock
import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.NewUnstableBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.core.apm.SpanType
import com.rarible.core.apm.withSpan
import com.rarible.core.apm.withTransaction
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
    private val blockEventListeners: List<BlockEventListener<BB>>,
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
        withTransaction(
            "handleBlock",
            labels = listOf(
                "blockNumber" to newBlockchainBlock.number,
                "blockHash" to newBlockchainBlock.hash,
                "blockParentHash" to (newBlockchainBlock.parentHash ?: "NONE"),
                "blockTimestamp" to newBlockchainBlock.timestamp
            )
        ) { handleNewBlock(newBlockchainBlock) }
    }

    private suspend fun handleNewBlock(newBlockchainBlock: BB) {
        val newBlock = newBlockchainBlock.toBlock()
        logger.info("Received new Block [{}:{}]: {}", newBlock.id, newBlock.hash, newBlock)

        val lastKnownBlock = getLastKnownBlock()
        logger.info("Last known Block [{}:{}]: {}", lastKnownBlock.id, lastKnownBlock.hash, lastKnownBlock)

        // We could omit heavy blockchain reorg check if the last known block is the parent of the new block.
        if (newBlock.parentHash == lastKnownBlock.hash && lastKnownBlock.status == BlockStatus.SUCCESS) {
            logger.info(
                "New block [{}:{}] is consistent with the last known block by parent hash {}",
                newBlock.id,
                newBlock.hash,
                lastKnownBlock.hash
            )
            logger.info("Processing the new consistent block [{}:{}]", newBlock.id, newBlock.hash)
            processBlock(newBlockchainBlock, false)
            return
        }

        restoreMissingBlocks(lastKnownBlock, newBlock)
    }

    private suspend fun restoreMissingBlocks(lastKnownBlock: Block, newBlock: Block) {
        val (lastCorrectBlock, lastCorrectBlockchainBlock) = withSpan(
            name = "findLatestCorrectKnownBlockAndRevertOther",
            labels = listOf("lastKnownBlockNumber" to lastKnownBlock.id, "newBlockNumber" to newBlock.id)
        ) {
            findLatestCorrectKnownBlockAndRevertOther(lastKnownBlock)
        }
        var currentBlock = lastCorrectBlock
        if (currentBlock.status == BlockStatus.PENDING) {
            logger.info(
                "Processing the latest correct block [{}:{}]",
                currentBlock.id,
                currentBlock.hash
            )
            currentBlock = processBlock(lastCorrectBlockchainBlock, false)
        }
        if (batchLoad.enabled && newBlock.id - currentBlock.id > batchLoad.confirmationBlockDistance) {
            logger.info(
                "Last correct block [{}:{}] is far away from the new block [{}:{}]. Using batch sync for the old blocks",
                currentBlock.id,
                currentBlock.hash,
                newBlock.id,
                newBlock.hash
            )
            val limit = newBlock.id - currentBlock.id - batchLoad.confirmationBlockDistance
            val batchSyncedBlock = withSpan(
                name = "batchSyncMissingBlocks",
                labels = listOf("lastKnownBlock" to lastKnownBlock, "limit" to limit)
            ) {
                batchSyncMissingBlocks(lastKnownBlock = currentBlock, limit = limit)
            }
            currentBlock = batchSyncedBlock ?: currentBlock
        }
        while (currentBlock.id < newBlock.id) {
            val nextBlock = getNextBlockchainBlock(currentBlock.id, newBlock.id - currentBlock.id)
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
                    nextBlock.number,
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
    private suspend fun findLatestCorrectKnownBlockAndRevertOther(lastKnownBlock: Block): Pair<Block, BB> {
        var currentBlock = lastKnownBlock
        var blockchainBlock = fetchBlock(currentBlock.id)

        while (blockchainBlock == null || currentBlock.hash != blockchainBlock.hash) {
            revertBlock(currentBlock)
            currentBlock = getPreviousBlock(currentBlock)
            blockchainBlock = fetchBlock(currentBlock.id)
        }

        return currentBlock to blockchainBlock
    }

    private suspend fun getLastKnownBlock(): Block {
        val lastKnownBlock = blockService.getLastBlock()
        if (lastKnownBlock != null) {
            return lastKnownBlock
        }

        logger.info("Fetching root block because there is no last known block")
        val firstBlock = blockClient.getFirstAvailableBlock()

        return processBlock(firstBlock, false)
    }

    private suspend fun getNextBlockchainBlock(startId: Long, maxSteps: Long): BB? {
        var id = startId
        val maxId = startId + maxSteps

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
        val finishId = lastKnownBlock.id + limit
        logger.info("Syncing missing blocks starting from $lastKnownBlock in batches of size ${batchLoad.batchSize} up to $finishId")
        BlockRanges.getRanges(
            from = lastKnownBlock.id + 1,
            to = finishId,
            step = batchLoad.batchSize
        )
            .map { range ->
                logger.info("Fetching blockchain blocks $range (${range.last - range.first + 1})")
                withSpan(
                    name = "fetchBlocksBatch",
                    type = SpanType.EXT,
                    labels = listOf("range" to range.toString())
                ) {
                    range.map { id -> async { fetchBlock(id) } }.awaitAll()
                }
            }
            .map { it.filterNotNull() }
            .filter { it.isNotEmpty() }
            .map {
                val fromId = it.first().number
                val toId = it.last().number
                logger.info("Processing batch of ${it.size} stable blocks: $fromId..$toId (${toId - fromId + 1}")
                withSpan(
                    name = "processBlocks",
                    labels = listOf("range" to "$fromId..$toId")
                ) {
                    processBlocks(it, true)
                }
            }
            .lastOrNull()
            ?.lastOrNull()
    }

    private suspend fun processBlocks(blocks: List<BB>, stable: Boolean): List<Block> {
        // TODO: implement this function for the whole batch.
        //  Currently, we can have only one PENDING block at the same time, so we have to process them one by one.
        return blocks.map { processBlock(it, stable) }
    }

    private suspend fun processBlock(blockchainBlock: BB, stable: Boolean): Block {
        logger.info("Processing block [{}:{}]", blockchainBlock.number, blockchainBlock.hash)
        blockService.save(blockchainBlock.toBlock(status = BlockStatus.PENDING))
        val event = if (stable) {
            NewStableBlockEvent(blockchainBlock)
        } else {
            NewUnstableBlockEvent(blockchainBlock)
        }
        withSpan(
            name = "processBlock",
            labels = listOf(
                "blockNumber" to blockchainBlock.number,
                "blockHash" to blockchainBlock.hash
            )
        ) {
            processBlockEvents(listOf(event))
        }
        return blockService.save(blockchainBlock.toBlock(status = BlockStatus.SUCCESS))
    }

    private suspend fun revertBlock(block: Block) {
        logger.info("Reverting block [{}:{}]: {}", block.id, block.hash, block)
        withSpan(
            name = "revertBlock",
            labels = listOf("blockNumber" to block.id)
        ) {
            processBlockEvents(listOf(RevertedBlockEvent(number = block.id, hash = block.hash)))
            blockService.remove(block.id)
        }
    }

    private suspend fun processBlockEvents(blockEvents: List<BlockEvent<BB>>) = coroutineScope {
        blockEventListeners.map { async { it.process(blockEvents) } }.awaitAll()
    }

    private suspend fun fetchBlock(number: Long): BB? =
        withSpan(
            name = "fetchBlock",
            type = SpanType.EXT,
            labels = listOf("blockNumber" to number)
        ) {
            blockClient.getBlock(number)
        }

}
