package com.rarible.blockchain.scanner.event.block

import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.util.BlockRanges
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.last
import kotlinx.coroutines.flow.onEach
import org.slf4j.LoggerFactory

/**
 * Synchronizes block state in db with the blockchain.
 * Emits these events:
 *  * NewBlock (when recent block found)
 *  * NewBlocks (when batch of old blocks found)
 *  * RevertedBlock (when blockchain state was reorganized, fork was detected)
 *
 * Blocks have 2 statuses:
 *  1. PENDING - when block is found but block processing is not yet finished (some or no data is processed)
 *  2. SUCCESS - when block is fully processed
 *
 * Considerations about handling statuses of the blocks:
 *  1. When new Block found:
 *      * block saved into the state with status PENDING
 *      * [blockListener] is notified about the event (NewBlock or NewBlocks)
 *      * if listener completes without an error, status is set to SUCCESS
 *  2. When block needs to be reverted (in blockchain hash of the block with the same number differs)
 *      * it should be reverted if block is in any status (PENDING or SUCCESS)
 *      * [blockListener] is notified about the event (RevertedBlock)
 *      * if listener completed without an error, block is removed from the db
 *
 * See [onNewBlock] for more information about algorithm
 * TODO
 *  handle statuses
 *  add batch events
 *  we can add constraint to the DB: only one PENDING block can exist. do we need this?
 */
class BlockHandler<BB : BlockchainBlock>(
    private val blockClient: BlockchainBlockClient<BB>,
    private val blockService: BlockService,
    private val blockListener: BlockEventPublisher,
    private val batchLoad: BlockBatchLoadProperties
) {

    private val logger = LoggerFactory.getLogger(BlockHandler::class.java)

    // We can cache lastKnown block in order to avoid huge amount of DB requests in
    // regular case of block processing
    @Volatile
    //TODO do we need this volatile?
    private var lastBlock: Block? = null

    /**
     * Entry-point.
     * It's called when new block is seen by the [BlockScanner]
     *  1. If [BlockchainBlock.parentHash] of the new block is the same as the last known block's hash then [NewBlockEvent] is emitted
     *      * if last known block is in status PENDING then we process all the way from the start
     *  2. Otherwise, handler tries to reconstruct the state:
     *      * [checkChainReorgAndRevert] is called: checks if latest blocks in db should be reverted
     *      * [batchRestoreMissedBlocks] is called: if db state is far from the blockchain state (thus it's safe to consider these blocks won't be reverted or changed in any way)
     *      * loads block information one by one and emits corresponding [NewBlockEvent] events
     */
    suspend fun onNewBlock(newBlockchainBlock: BB) {
        logger.info("Received new Block [{}:{}]", newBlockchainBlock.number, newBlockchainBlock.hash)
        val newBlock = mapBlockchainBlock(newBlockchainBlock)

        val lastStateBlock = getLastKnownBlock()
        logger.info("Last known Block: $lastStateBlock")

        // If new block's parent hash is the same as hash of last known block, we could omit blockchain reorg check
        if (newBlockchainBlock.parentHash == lastStateBlock.hash) {
            if (lastStateBlock.status == BlockStatus.PENDING) {
                newBlock(lastStateBlock)
            }
            lastBlock = newBlock(newBlock)
            return
        }

        // Otherwise, we have missed blocks in our state OR chain was reorganized
        lastBlock = restoreMissingBlocks(lastStateBlock, newBlock)
    }

    private suspend fun restoreMissingBlocks(lastStateBlock: Block, newBlock: Block): Block {
        var currentBlock = checkChainReorgAndRevert(lastStateBlock)

        currentBlock = if (batchLoad.enabled && newBlock.id - currentBlock.id > batchLoad.confirmationBlockDistance) {
            batchRestoreMissedBlocks(currentBlock, newBlock.id - currentBlock.id - batchLoad.confirmationBlockDistance)
        } else {
            currentBlock
        }
        if (currentBlock.status == BlockStatus.PENDING) {
            newBlock(currentBlock)
        }
        while (currentBlock.id < newBlock.id) {
            val nextBlock = getNextBlock(currentBlock, newBlock.id - currentBlock.id)

            if (nextBlock.parentHash != currentBlock.hash) {
                // chain reorg occurred while we were going forward
                // stop this iteration. next time we will check reorg and go forward again
                logger.info(
                    "Chain has been reorganized during reading missed blocks: " +
                            "[{}:{}] is not a parent of [{}:{}], chain will be reorganized on next BlockEvent",
                    currentBlock.id, currentBlock.hash, newBlock.id, newBlock.hash
                )

                return currentBlock
            }

            currentBlock = newBlock(nextBlock)
        }
        return currentBlock
    }

    /**
     * Checks if chain reorg happened. Find the latest correct block and revert others in order
     * @param startStateBlock block from the state to start looking for reorg from
     * @return latest correct block found
     */
    private suspend fun checkChainReorgAndRevert(startStateBlock: Block): Block {
        var stateBlock = startStateBlock
        var blockchainBlock = fetchBlock(stateBlock.id)

        while (blockchainBlock == null || blockchainBlock.hash != stateBlock.hash) {
            revertBlock(stateBlock)

            stateBlock = getPreviousBlock(stateBlock)
            blockchainBlock = fetchBlock(stateBlock.id)
        }

        return stateBlock
    }

    private suspend fun getLastKnownBlock(): Block {
        if (lastBlock != null) {
            return lastBlock!!
        }
        val lastKnownBlock = blockService.getLastBlock()
        if (lastKnownBlock != null) {
            lastBlock = lastKnownBlock
            return lastKnownBlock
        }

        logger.info("There is no last know Block, retrieving first one")
        val blockchainBlock = newBlock(fetchBlock(0) ?: error("not found root block"))

        logger.info("Found first Block in chain [{}:{}]", blockchainBlock.id, blockchainBlock.hash)

        lastBlock = blockchainBlock
        return blockchainBlock
    }

    private suspend fun getNextBlock(startBlock: Block, maxSteps: Long): Block {
        var id = startBlock.id

        while (id < startBlock.id + maxSteps) {
            val block = fetchBlock(id + 1)

            if (block != null) return block

            id++
        }

        error("Can't find next block for: $startBlock")
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

    private suspend fun batchRestoreMissedBlocks(lastStateBlock: Block, amount: Long): Block = coroutineScope {
        val startBlockId = lastStateBlock.id + 1
        val endBlockId = startBlockId + amount
        require(endBlockId - startBlockId > 0) { "Block batch restore range must be positive" }

        logger.info("Restore blocks from $startBlockId to $endBlockId with batch requests")

        BlockRanges.getRanges(startBlockId, endBlockId, batchLoad.batchSize).flatMapConcat { range ->
            logger.info("Restore block range $range")
            range.map { id ->
                async {
                    fetchBlock(id) ?: throw IllegalStateException("Can't find restored block $id")
                }
            }.awaitAll().asFlow()
        }.onEach { block ->
            newBlock(block)
        }.last()
    }

    private suspend fun newBlock(block: Block): Block {
        blockService.save(block.copy(status = BlockStatus.PENDING))
        blockListener.publish(NewBlockEvent(Source.BLOCKCHAIN, block.id, block.hash))
        val result = blockService.save(block.copy(status = BlockStatus.SUCCESS))
        logger.info("Block [{}:{}] saved", block.id, block.hash)
        return result
    }

    private suspend fun revertBlock(block: Block) {
        blockListener.publish(RevertedBlockEvent(Source.BLOCKCHAIN, block.id, block.hash))
        blockService.remove(block.id)
    }

    private suspend fun fetchBlock(number: Long): Block? {
        val blockchainBlock = blockClient.getBlock(number)
        return blockchainBlock?.let { mapBlockchainBlock(it) }
    }
}

fun mapBlockchainBlock(b: BlockchainBlock): Block =
    Block(b.number, b.hash, b.parentHash, b.timestamp, BlockStatus.PENDING)
