package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.core.common.nowMillis
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.runBlocking
import java.time.Instant
import kotlin.math.max

class BlockMonitor(
    properties: BlockchainScannerProperties,
    meterRegistry: MeterRegistry,
    private val blockService: BlockService
) : AbstractMonitor(
    properties,
    meterRegistry,
    "block"
) {

    @Volatile
    private var lastBlock: Block? = null

    private var getBlockTimer: Timer? = null

    override fun register() {
        addGauge("delay") { getBlockDelay() }
        getBlockTimer = addTimer("get_block")
    }

    override fun refresh() = runBlocking {
        lastBlock = blockService.getLastBlock()
    }

    fun recordGetBlock(sample: Timer.Sample) {
        getBlockTimer?.let { sample.stop(it) }
    }

    fun getBlockDelay(now: Instant = nowMillis()): Double? {
        val lastSeenBlockTimestamp = lastBlock?.timestamp ?: return null
        val currentTimestamp = now.epochSecond
        return max(currentTimestamp - lastSeenBlockTimestamp, 0).toDouble()
    }
}
