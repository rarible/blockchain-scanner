package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.block.BlockStatus
import com.rarible.blockchain.scanner.block.Fail
import com.rarible.blockchain.scanner.block.toBlock
import com.rarible.blockchain.scanner.configuration.BlockBatchLoadProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.NewUnstableBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.core.common.asyncWithTraceId
import com.rarible.core.logging.withTraceId
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.runningFold
import kotlinx.coroutines.flow.takeWhile
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference

/**
 * Synchronizes database with the blockchain, see [onNewBlock].
 */
class BlockHandler<BB : BlockchainBlock>(
    private val blockClient: BlockchainBlockClient<BB>,
    private val blockService: BlockService,
    private val blockEventListeners: List<BlockEventListener<BB>>,
    private val scanProperties: ScanProperties,
    private val monitor: BlockMonitor
) {

    private val logger = LoggerFactory.getLogger(BlockHandler::class.java)

    init {
        logger.info("Creating BlockHandler with config: ${scanProperties.batchLoad}")
    }

    suspend fun onNewBlock(newBlockchainBlock: BB) {
        monitor.onBlockEvent {
            onNewBlockHandler(newBlockchainBlock)
        }
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
    private suspend fun onNewBlockHandler(newBlockchainBlock: BB) {
        val newBlock = newBlockchainBlock.toBlock()
        logger.info("Received new Block: {}", newBlock)

        val lastKnownBlock = getLastKnownBlock()
        logger.info("Last known Block: {}", lastKnownBlock)

        val alreadyIndexedBlock = blockService.getBlock(newBlock.id)
        if (alreadyIndexedBlock != null) {
            if (newBlock == alreadyIndexedBlock.copy(status = newBlock.status, stats = null)) {
                logger.info("The new Block is already indexed, skipping it: {}", newBlock)
                return
            }
            logger.warn("The new Block {} was already indexed but differs from {}", newBlock, alreadyIndexedBlock)
        }

        // In case if we got block following after the last known,
        val lastCorrectBlock = if (
            lastKnownBlock.id + 1 == newBlockchainBlock.number &&
            lastKnownBlock.hash == newBlockchainBlock.parentHash &&
            lastKnownBlock.status == BlockStatus.SUCCESS
        ) {
            lastKnownBlock
        } else {
            findLatestCorrectKnownBlockAndRevertOther(lastKnownBlock)
        }

        logger.info("Syncing missing blocks from the last correct known block {} to {}", lastCorrectBlock, newBlock)
        val stableUnstableBlockRanges = BlockRanges.getStableUnstableBlockRanges(
            baseBlockNumber = lastCorrectBlock.id,
            lastBlockNumber = newBlock.id,
            batchSize = scanProperties.batchLoad.batchSize,
            stableDistance = scanProperties.batchLoad.confirmationBlockDistance
        )
        coroutineScope {
            val channel = produceBlocks(
                blockRanges = stableUnstableBlockRanges,
                baseBlock = lastCorrectBlock,
                capacity = scanProperties.batchLoad.batchBufferSize
            )

            val lastSyncedBlockReference = AtomicReference<Block>()

            for (batch in channel) {
                val lastSyncedBlock = processBlocks(
                    blocksBatch = batch,
                    mode = ScanMode.REALTIME,
                    trigger = newBlockchainBlock,
                ).last()
                lastSyncedBlockReference.set(lastSyncedBlock)
                monitor.recordLastIndexedBlock(lastSyncedBlock)
            }
            if (lastSyncedBlockReference.get() != null) {
                val lastSyncedBlock = lastSyncedBlockReference.get()
                logger.info(
                    "Syncing completed {} on block: {}",
                    if (lastSyncedBlock.id == newBlock.id) "fully" else "prematurely",
                    lastSyncedBlock
                )
            } else {
                logger.info("Syncing completed prematurely")
            }
        }
    }

    @Deprecated("Used only by Solana, should be removed")
    fun syncBlocks(
        blocks: List<Long>
    ): Flow<Block> = flow {
        for (block in blocks) {
            val blockchainBlock = blockClient.getBlock(block) ?: error("Can't get block: $block")

            emit(
                processBlocks(
                    blocksBatch = BlocksBatch(
                        blocksRange = TypedBlockRange(
                            range = blockchainBlock.number..blockchainBlock.number,
                            stable = false
                        ),
                        blocks = listOf(blockchainBlock)
                    ),
                    mode = ScanMode.REINDEX
                ).single()
            )
        }
    }

    @Suppress("EXPERIMENTAL_API_USAGE")
    fun syncBlocks(
        blockRanges: Flow<TypedBlockRange>,
        baseBlock: Block,
        mode: ScanMode
    ): Flow<Block> = blockRanges
        .runningFold(baseBlock as Block?) { lastProcessedBlock, blocksRange ->
            if (lastProcessedBlock == null) {
                // Some blocks in the previous batches are inconsistent by parent-child hash.
                return@runningFold null
            }
            val (blocksBatch, parentHashesAreConsistent) = fetchBlocksBatchWithHashConsistencyCheck(
                lastProcessedBlockHash = lastProcessedBlock.hash,
                blocksRange = blocksRange
            )
            val newLastProcessedBlock = if (blocksBatch.blocks.isNotEmpty()) {
                processBlocks(blocksBatch, mode).last()
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
        .drop(1) // Drop the baseBlock (the first item in the runningFold's Flow)
        .filterNotNull()

    @Suppress("EXPERIMENTAL_API_USAGE", "OPT_IN_USAGE")
    private fun CoroutineScope.produceBlocks(
        blockRanges: Sequence<TypedBlockRange>,
        baseBlock: Block,
        capacity: Int
    ) = produce(capacity = capacity) {
        val lastFetchedBlockHash = AtomicReference(baseBlock.hash)

        blockRanges.chunked(capacity / 2).asFlow().withTraceId().takeWhile { batchOfRanges ->
            val batchOfBlockBatches = coroutineScope {
                batchOfRanges.map { range ->
                    asyncWithTraceId {
                        fetchBlocksByRange(range)
                    }
                }
            }.awaitAll()
            for (blocksBatch in batchOfBlockBatches) {
                val (batch, isConsistent) = checkBlocksBatchHashConsistency(
                    baseBlockHash = lastFetchedBlockHash.get(),
                    blocksBatch = blocksBatch
                )
                if (batch.blocks.isNotEmpty()) {
                    val last = batch.blocks.last()
                    lastFetchedBlockHash.set(last.hash)
                    monitor.recordLastFetchedBlockNumber(last.number)

                    send(batch)
                }
                if (!isConsistent) {
                    return@takeWhile false
                }
            }
            true
        }.collect()
    }

    /**
     * Checks if chain reorg happened. Find the latest correct block having status SUCCESS
     * and revert others in the reversed order (from the newest to the oldest).
     * Returns the latest correct known block with status SUCCESS.
     */
    private suspend fun findLatestCorrectKnownBlockAndRevertOther(lastKnownBlock: Block): Block {
        val currentBlock = AtomicReference(lastKnownBlock)
        val blockchainBlock = AtomicReference(blockClient.getBlock(lastKnownBlock.id))

        val condition: () -> Boolean = {
            val b = blockchainBlock.get()
            val c = currentBlock.get()
            b == null || c.hash != b.hash || c.status == BlockStatus.PENDING
        }

        while (condition()) {
            val current = currentBlock.get()
            revertBlock(current)
            val previous = getPreviousBlock(current)
            currentBlock.set(previous)
            blockchainBlock.set(blockClient.getBlock(previous.id))
        }

        return currentBlock.get()
    }

    private suspend fun getLastKnownBlock(): Block {
        val lastKnownBlock = blockService.getLastBlock()
        if (lastKnownBlock != null) {
            return lastKnownBlock
        }

        val firstBlock = blockClient.getFirstAvailableBlock()
        logger.info("Processing the first Block {}", firstBlock)

        return processBlocks(
            blocksBatch = BlocksBatch(
                blocksRange = TypedBlockRange(
                    range = firstBlock.number..firstBlock.number,
                    stable = true
                ),
                blocks = listOf(firstBlock)
            ),
            mode = ScanMode.REALTIME
        ).single()
    }

    private suspend fun getPreviousBlock(startBlock: Block): Block {
        var id = startBlock.id
        while (id > 0) {
            val block = blockService.getBlock(id - 1)
            if (block != null) return block
            id--
        }
        error("Can't find previous Block for: $startBlock")
    }

    private data class BlocksBatch<BB>(
        val blocksRange: TypedBlockRange,
        val blocks: List<BB>
    ) {
        override fun toString(): String = "$blocksRange (present ${blocks.size})"
    }

    private suspend fun fetchBlocksBatchWithHashConsistencyCheck(
        lastProcessedBlockHash: String,
        blocksRange: TypedBlockRange
    ): Pair<BlocksBatch<BB>, Boolean> {
        val blocksBatch = fetchBlocksByRange(blocksRange)
        return checkBlocksBatchHashConsistency(
            baseBlockHash = lastProcessedBlockHash,
            blocksBatch = blocksBatch
        )
    }

    private suspend fun fetchBlocksByRange(blocksRange: TypedBlockRange): BlocksBatch<BB> {
        val fetchedBlocks = blockClient.getBlocks(blocksRange.range.toList())
        return BlocksBatch(blocksRange, fetchedBlocks)
    }

    /**
     * Checks parent-child hash consistency of the [blocksBatch].
     * The first block of the [blocksBatch] must be the child block of the [baseBlockHash].
     * If all the blocks in the range are consistent, returns the [blocksBatch] and `true`.
     * Otherwise, returns the prefix of consistent blocks and `false`.
     */
    private fun checkBlocksBatchHashConsistency(
        baseBlockHash: String,
        blocksBatch: BlocksBatch<BB>
    ): Pair<BlocksBatch<BB>, Boolean> {
        val fetchedBlocks = blocksBatch.blocks
        if (fetchedBlocks.isEmpty()) {
            return BlocksBatch(
                blocksRange = TypedBlockRange(range = LongRange.EMPTY, stable = blocksBatch.blocksRange.stable),
                blocks = emptyList<BB>()
            ) to true
        }
        var parentBlockHash = baseBlockHash
        val blocks = arrayListOf<BB>()
        var consistentRange = blocksBatch.blocksRange
        for (block in fetchedBlocks) {
            if (parentBlockHash != block.parentHash) {
                consistentRange = TypedBlockRange(
                    range = fetchedBlocks.first().number until block.number,
                    stable = blocksBatch.blocksRange.stable
                )
                logger.warn(
                    "Blocks ${blocksBatch.blocksRange} have inconsistent parent-child hash chain on block ${block.number}:${block.hash}, " +
                        "parent hash must be $parentBlockHash but was ${block.parentHash}, " +
                        "the consistent range is $consistentRange"
                )
                break
            }
            blocks += block
            parentBlockHash = block.hash
        }
        val parentHashesAreConsistent = consistentRange == blocksBatch.blocksRange
        return BlocksBatch(consistentRange, blocks) to parentHashesAreConsistent
    }

    private suspend fun processBlocks(
        blocksBatch: BlocksBatch<BB>,
        mode: ScanMode,
        trigger: BB? = null
    ): List<Block> {
        val (blocksRange, blocks) = blocksBatch
        logger.info("Processing $blocksBatch")
        if (!blocksRange.stable) {
            /*
            There is no need to save PENDING status for stable blocks.
            On restart, the scanner will process the same stable blocks and send the same events.
            The clients of the blockchain scanner are ready to receive duplicated events.
             */
            blocks.map { blockService.save(it.toBlock(BlockStatus.PENDING)) }
        }

        val blockEvents = blocks.map {
            val block = it.withReceivedTime(trigger)
            if (blocksRange.stable) {
                NewStableBlockEvent(block, mode)
            } else {
                NewUnstableBlockEvent(block, mode)
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
        val processed = processBlockEvents(blockEvents, mode)

        val toSave = blocks.map { block ->
            val blockResult = processed[block.number]!!
            val stats = blockResult.map { it.stats }.reduce { a, b -> a.merge(b) }
            val errors = blockResult.flatMap { it.errors }.map { Fail(it.groupId, it.errorMessage) }
            val status = if (errors.isEmpty()) BlockStatus.SUCCESS else BlockStatus.ERROR
            block.toBlock(status, stats, errors)
        }

        return if (blocksRange.stable) {
            saveStableBlocks(toSave, blocksRange, mode)
        } else {
            blockService.save(toSave)
        }
    }

    private suspend fun saveStableBlocks(
        blocks: List<Block>,
        blocksRange: TypedBlockRange,
        mode: ScanMode
    ): List<Block> {
        val result = withExceptionLogging("Save $blocksRange") {
            when (mode) {
                // When we are reading forward, only new blocks should be there
                ScanMode.REALTIME -> blockService.insert(blocks)
                // For full reindex we can overwrite all blocks with status/stats
                ScanMode.REINDEX -> blockService.save(blocks)
                // For partial reindex we should NOT overwrite block stats (only except the case block is missing)
                ScanMode.REINDEX_PARTIAL -> blockService.insertMissing(blocks)
                ScanMode.RECONCILIATION -> blocks
            }
        }
        return result
    }

    private suspend fun revertBlock(block: Block) {
        logger.info("Reverting block: {}", block)
        // During revert operation we don't expect any errors, if it happened - it means something is completely broken
        processBlockEvents(
            listOf(
                RevertedBlockEvent(
                    number = block.id,
                    hash = block.hash,
                    mode = ScanMode.REALTIME
                )
            ),
            mode = ScanMode.REALTIME
        )
        blockService.remove(block.id)
    }

    private suspend fun processBlockEvents(
        blockEvents: List<BlockEvent<BB>>,
        mode: ScanMode
    ): Map<Long, List<BlockEventResult>> = coroutineScope {
        monitor.onProcessBlocksEvents {
            // Here we process events first, just to store data to DB
            val listenersResult = blockEventListeners.map {
                asyncWithTraceId(context = NonCancellable) { it.process(blockEvents) }
            }.awaitAll()

            val result = listenersResult.flatMap { it.blocks }.groupBy { it.blockNumber }

            // blockNumber -> list of errors
            val errors = result.mapValues { it.value.flatMap(BlockEventResult::errors) }
                .filter { it.value.isNotEmpty() }

            if (errors.isNotEmpty()) {
                // For non-realtime scan there should not be errors at all, otherwise reindex doesn't make sense
                if (mode != ScanMode.REALTIME) {
                    throw IllegalStateException("Failed to process blocks ${errors.keys.toSortedSet()}")
                }
                val currentFailedBlockCount = blockService.countFailed()
                if (currentFailedBlockCount + errors.size > scanProperties.maxFailedBlocksAllowed) {
                    throw IllegalStateException(
                        "Failed to process ${errors.size} blocks (${errors.map { it.key }})," +
                            " and there are already $currentFailedBlockCount failed blocks"
                    )
                }
            }

            // And only if everything processed successfully, we're publishing logs to Kafka
            listenersResult.map { asyncWithTraceId(context = NonCancellable) { it.publish() } }.awaitAll()

            result
        }
    }

    private fun BB.withReceivedTime(trigger: BB?): BB {
        @Suppress("UNCHECKED_CAST")
        return if (trigger?.number == this.number) this.withReceivedTime(trigger.receivedTime) as BB else this
    }
}
