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
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.core.apm.SpanType
import com.rarible.core.apm.withSpan
import com.rarible.core.apm.withTransaction
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.lastOrNull
import kotlinx.coroutines.flow.runningFold
import org.slf4j.LoggerFactory

/**
 * Synchronizes database with the blockchain, see [onNewBlock].
 */
class BlockHandler<BB : BlockchainBlock>(
    private val blockClient: BlockchainBlockClient<BB>,
    private val blockService: BlockService,
    private val blockEventListeners: List<BlockEventListener<BB>>,
    private val batchLoad: BlockBatchLoadProperties,
    private val monitor: BlockMonitor
) {

    private val logger = LoggerFactory.getLogger(BlockHandler::class.java)

    init {
        logger.info("Creating BlockHandler with config: $batchLoad")
    }

    /**
     * Handle a new block event and synchronize the database state with blockchain.
     *
     * Implementation of this method is approximately as follows:
     * 1) Firstly, we find the latest known block that is synced with the blockchain (by comparing block hashes)
     * and revert all out-of-sync blocks in the reversed order (from the newest to the oldest).
     * 2) Secondly, we process blocks from the last known correct block to the newest block,
     * we use batches for performance and there is distinction for "stable" and not yet stable blocks,
     * which is determined by [BlockBatchLoadProperties.confirmationBlockDistance]. Ethereum client,
     * for example, can return block results faster for the stable blocks.
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

        val lastCorrectBlock = withTransaction(
            name = "findLatestCorrectKnownBlockAndRevertOther",
            labels = listOf("lastKnownBlockNumber" to lastKnownBlock.id, "newBlockNumber" to newBlock.id)
        ) {
            findLatestCorrectKnownBlockAndRevertOther(lastKnownBlock)
        }

        logger.info("Syncing missing blocks from the last correct known block {} to {}", lastCorrectBlock, newBlock)
        val lastSyncedBlock = syncBlocks(lastCorrectBlock, newBlock).lastOrNull()
        if (lastSyncedBlock != null) {
            logger.info(
                "Syncing completed {} on block [{}:{}]: {}",
                if (lastSyncedBlock.id == newBlock.id) "fully" else "prematurely",
                lastSyncedBlock.id,
                lastSyncedBlock.hash,
                lastSyncedBlock
            )
        } else {
            logger.info("Syncing completed prematurely")
        }
    }

    @Suppress("EXPERIMENTAL_API_USAGE")
    private fun syncBlocks(
        lastCorrectBlock: Block,
        newBlock: Block
    ): Flow<Block> = BlockRanges.getStableUnstableBlockRanges(
        lastKnownBlockNumber = lastCorrectBlock.id,
        newBlockNumber = newBlock.id,
        batchSize = batchLoad.batchSize,
        stableDistance = batchLoad.confirmationBlockDistance
    )
        .asFlow()
        .runningFold(lastCorrectBlock as Block?) { lastProcessedBlock, blocksRange ->
            if (lastProcessedBlock == null) {
                // Some blocks in the previous batches are inconsistent by parent-child hash.
                return@runningFold null
            }
            val (blocksBatch, parentHashesAreConsistent) = fetchBlocksBatchWithHashConsistencyCheck(
                lastProcessedBlock = lastProcessedBlock,
                blocksRange = blocksRange
            )
            val newLastProcessedBlock = if (blocksBatch.blocks.isNotEmpty()) {
                processBlocks(blocksBatch).last()
            } else {
                null
            }
            if (parentHashesAreConsistent) {
                newLastProcessedBlock ?: lastProcessedBlock
            } else {
                // Skip further batches from processing.
                null
            }
        }
        .drop(1) // Drop the lastCorrectBlock
        .filterNotNull()

    /**
     * Checks if chain reorg happened. Find the latest correct block having status SUCCESS
     * and revert others in the reversed order (from the newest to the oldest).
     * Returns the latest correct known block with status SUCCESS.
     */
    private suspend fun findLatestCorrectKnownBlockAndRevertOther(lastKnownBlock: Block): Block {
        var currentBlock = lastKnownBlock
        var blockchainBlock = fetchBlock(currentBlock.id)

        while (
            blockchainBlock == null
            || currentBlock.hash != blockchainBlock.hash
            || currentBlock.status == BlockStatus.PENDING
        ) {
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

        val firstBlock = blockClient.getFirstAvailableBlock()
        logger.info("Processing the first block [{}:{}]", firstBlock.number, firstBlock.hash)

        return processBlocks(
            BlocksBatch(
                blocksRange = BlocksRange(
                    range = firstBlock.number..firstBlock.number,
                    stable = true
                ),
                blocks = listOf(firstBlock)
            )
        ).single()
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

    private data class BlocksBatch<BB>(val blocksRange: BlocksRange, val blocks: List<BB>) {
        override fun toString(): String = "$blocksRange (present ${blocks.size})"
    }

    private suspend fun fetchBlocksBatchWithHashConsistencyCheck(
        lastProcessedBlock: Block,
        blocksRange: BlocksRange
    ): Pair<BlocksBatch<BB>, Boolean /* Whether all parent-child hashes match */> = coroutineScope {
        logger.info("Fetching $blocksRange")
        val fetchedBlocks = withTransaction(
            name = "fetchBlocks",
            labels = listOf("range" to blocksRange.range.toString())
        ) {
            blocksRange.range.map { id -> async { fetchBlock(id) } }.awaitAll().filterNotNull()
        }
        logger.info("Fetched ${fetchedBlocks.size} for $blocksRange")
        if (fetchedBlocks.isEmpty()) {
            return@coroutineScope BlocksBatch(BlocksRange(LongRange.EMPTY, blocksRange.stable), emptyList<BB>()) to true
        }
        var parentBlockHash = lastProcessedBlock.hash
        val blocks = arrayListOf<BB>()
        var consistentRange = blocksRange
        for (block in fetchedBlocks) {
            if (parentBlockHash != block.parentHash) {
                consistentRange = BlocksRange(
                    range = blocksRange.range.first until block.number,
                    stable = blocksRange.stable
                )
                logger.info(
                    "Blocks $blocksRange have inconsistent parent-child hash chain on block ${block.number}:${block.hash}, " +
                            "parent hash must be $parentBlockHash but was ${block.parentHash}, " +
                            "the consistent range is $consistentRange"
                )
                break
            }
            blocks += block
            parentBlockHash = block.hash
        }
        val parentHashesAreConsistent = consistentRange == blocksRange
        BlocksBatch(consistentRange, blocks) to parentHashesAreConsistent
    }

    private suspend fun processBlocks(blocksBatch: BlocksBatch<BB>): List<Block> {
        val (blocksRange, blocks) = blocksBatch
        logger.info("Processing $blocksBatch")
        return withTransaction(
            name = "processBlocks",
            labels = listOf(
                "size" to blocks.size,
                "minId" to blocksRange.range.first,
                "maxId" to blocksRange.range.last
            )
        ) {
            if (!blocksRange.stable) {
                /*
                There is no need to save PENDING status for stable blocks.
                On restart, the scanner will process the same stable blocks and send the same events.
                The clients of the blockchain scanner are ready to receive duplicated events.
                 */
                saveBlocks(blocks, BlockStatus.PENDING)
            }
            val blockEvents = blocks.map {
                if (blocksRange.stable) {
                    NewStableBlockEvent(it)
                } else {
                    NewUnstableBlockEvent(it)
                }
            }

            /*
             It may rarely happen that we produce RevertedBlockEvent-s for blocks for which we DID NOT produce NewBlockEvent-s.
             This happens if at this exact line, before calling "processBlockEvents", the blockchain scanner gets terminated.
             The blocks are already marked as PENDING.
             After restart, we will produce RevertedBlockEvent-s, although we did not call
             "processBlockEvents" with NewBlockEvent-s for those blocks.

             This is OK because handling of reverted blocks simply reverts logs that we might have had chance to record.
             If we did not call NewBlockEvent-s, there will be nothing to revert.
            */
            processBlockEvents(blockEvents)
            saveBlocks(blocks, BlockStatus.SUCCESS)
        }
    }

    private suspend fun saveBlocks(blocks: List<BB>, status: BlockStatus): List<Block> =
        blocks.map {
            val block = it.toBlock(status)
            blockService.save(block)
            logger.info("Saved block $block")
            block
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
        withSpan(
            name = "processBlocks",
            labels = listOf(
                "count" to blockEvents.size,
                "minId" to blockEvents.first().number,
                "maxId" to blockEvents.last().number
            )
        ) {
            blockEventListeners.map { async { it.process(blockEvents) } }.awaitAll()
        }
    }

    private suspend fun fetchBlock(number: Long): BB? =
        withSpan(
            name = "fetchBlock",
            type = SpanType.EXT,
            labels = listOf("blockNumber" to number)
        ) {
            val sample = Timer.start()

            try {
                blockClient.getBlock(number)
            } finally {
                monitor.recordGetBlock(sample)
            }
        }
}
