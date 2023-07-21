package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.TimestampUnit
import com.rarible.core.common.nowMillis
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Instant
import kotlin.math.max

class BlockMonitor(
    properties: BlockchainScannerProperties,
    meterRegistry: MeterRegistry
) : AbstractMonitor(
    properties,
    meterRegistry,
    "block"
) {

    @Volatile
    private var lastIndexedBlock: Block? = null

    @Volatile
    private var lastLoadedBlockNumber: Long? = null

    private var getBlockTimer: Timer? = null

    private var getBlocksTimer: Timer? = null

    private var processBlocksEventsTimer: Timer? = null

    private var onBlockTimer: Timer? = null

    private val timestampUnit = properties.monitoring.timestampUnit

    override fun register() {
        addGauge("delay") { getBlockDelay() }
        addGauge("last_block_number") { lastIndexedBlock?.id }
        addGauge("last_loaded_block_number") { lastLoadedBlockNumber }

        getBlockTimer = addTimer("get_block")
        getBlocksTimer = addTimer("get_blocks")
        processBlocksEventsTimer = addTimer("process_blocks_events")
        onBlockTimer = addTimer("on_block")
    }

    override fun refresh() = Unit

    fun recordLastIndexedBlock(lastIndexedBlock: Block) {
        this.lastIndexedBlock = lastIndexedBlock
    }

    fun recordLastFetchedBlockNumber(lastFetchedBlockNumber: Long) {
        this.lastLoadedBlockNumber = lastFetchedBlockNumber
    }

    suspend fun <T> onGetBlocks(block: suspend () -> T): T {
        return recordTime(getBlockTimer, block)
    }

    suspend fun <T> onProcessBlocksEvents(handler: suspend () -> T): T {
        return recordTime(processBlocksEventsTimer, handler)
    }

    suspend fun onBlockEvent(handler: suspend () -> Unit) {
        onBlockTimer?.let { recordTime(it, handler) }
    }

    private fun getBlockDelay(now: Instant = nowMillis()): Double? {
        val lastSeenBlockTimestamp = lastIndexedBlock?.timestamp ?: return null

        val currentTimestamp = when (timestampUnit) {
            TimestampUnit.SECOND -> now.epochSecond
            TimestampUnit.MILLISECOND -> now.toEpochMilli()
        }
        return max(currentTimestamp - lastSeenBlockTimestamp, 0).toDouble()
    }
}
