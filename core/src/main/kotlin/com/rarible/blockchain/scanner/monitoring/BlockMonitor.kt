package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.TimestampUnit
import com.rarible.core.common.nowMillis
import io.micrometer.core.instrument.MeterRegistry
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.max

class BlockMonitor(
    private val blockRepository: BlockRepository,
    properties: BlockchainScannerProperties,
    meterRegistry: MeterRegistry,
) : AbstractMonitor(
    properties,
    meterRegistry,
    "block"
) {
    private var lastIndexedBlock = AtomicReference<Block?>(null)

    @Volatile
    private var lastLoadedBlockNumber: Long? = null

    @Volatile
    private var blockErrors: Long = 0L

    private val timestampUnit = properties.monitoring.timestampUnit

    override fun register() {
        addGauge(BLOCK_DELAY) { getBlockDelay() }
        addGauge(LAST_BLOCK_NUMBER) { lastIndexedBlock.get()?.id }
        addGauge(LAST_LOADED_BLOCK_NUMBER) { lastLoadedBlockNumber }
        addGauge(BLOCK_ERRORS) { blockErrors }
    }

    override suspend fun refresh() {
        blockErrors = blockRepository.failedCount()
        setLastIndexedBlock()
    }

    fun recordLastIndexedBlock(lastIndexedBlock: Block) {
        this.lastIndexedBlock.set(lastIndexedBlock)
    }

    fun recordLastFetchedBlockNumber(lastFetchedBlockNumber: Long) {
        this.lastLoadedBlockNumber = lastFetchedBlockNumber
    }

    inline fun <T> onProcessBlocksEvents(handler: () -> T): T {
        return recordTime(getTimer(PROCESS_BLOCKS_EVENTS), handler)
    }

    inline fun onBlockEvent(handler: () -> Unit) {
        recordTime(getTimer(ON_BLOCK), handler)
    }

    private fun getBlockDelay(now: Instant = nowMillis()): Double? {
        val lastSeenBlockTimestamp = lastIndexedBlock.get()?.timestamp ?: return null

        val currentTimestamp = when (timestampUnit) {
            TimestampUnit.SECOND -> now.epochSecond
            TimestampUnit.MILLISECOND -> now.toEpochMilli()
        }
        return max(currentTimestamp - lastSeenBlockTimestamp, 0).toDouble()
    }

    private suspend fun setLastIndexedBlock() {
        val currentLastIndexedBlock = lastIndexedBlock.get()
        if (currentLastIndexedBlock == null) {
            // We assume that the last block in the database is the current state of the scanner, regardless of the block's condition.
            val last = blockRepository.getLastBlock()
            lastIndexedBlock.compareAndSet(null, last)
        }
    }

    internal fun getLastIndexedBlock(): Block? {
        return lastIndexedBlock.get()
    }

    companion object {
        const val BLOCK_DELAY = "delay"
        const val LAST_BLOCK_NUMBER = "last_block_number"
        const val LAST_LOADED_BLOCK_NUMBER = "last_loaded_block_number"
        const val BLOCK_ERRORS = "block_error"

        const val PROCESS_BLOCKS_EVENTS = "process_blocks_events"
        const val ON_BLOCK = "on_block"
    }
}
