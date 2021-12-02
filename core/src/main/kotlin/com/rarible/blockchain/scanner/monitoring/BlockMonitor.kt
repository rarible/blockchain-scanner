package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.runBlocking
import java.time.Instant
import kotlin.math.max

class BlockMonitor(
    properties: BlockchainScannerProperties,
    meterRegistry: MeterRegistry,
    private val blockService: BlockService<*>
) : AbstractMonitor(
    properties,
    meterRegistry,
    "block"
) {

    @Volatile
    private var lastBlock: Block? = null

    override fun register() {
        addGauge("delay") { getBlockDelay() }
    }

    override fun refresh() = runBlocking {
        lastBlock = blockService.getLastBlock()
    }

    fun getBlockDelay(): Number? {
        val lastSeenBlockTimestamp = lastBlock?.timestamp ?: return null
        val currentTimestamp = Instant.now().epochSecond
        return max(currentTimestamp - lastSeenBlockTimestamp, 0).toDouble()
    }
}