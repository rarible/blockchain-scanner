package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.runBlocking
import java.time.Instant
import java.util.function.Supplier
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

    @Volatile
    private var blocksWithErrorsStatus: Long = 0

    @Volatile
    private var blocksWithPendingStatus: Long = 0

    override fun register() {
        addGauge("delay", Supplier { getBlockDelay() })
        addGauge("error", Supplier { getErrorBlockCount() })
        addGauge("pending", Supplier { getPendingBlockCount() })
    }

    override fun refresh() = runBlocking {
        lastBlock = blockService.getLastBlock()
        blocksWithErrorsStatus = blockService.findByStatus(Block.Status.ERROR).count().toLong()
        blocksWithPendingStatus = blockService.findByStatus(Block.Status.PENDING).count().toLong()
    }

    fun getBlockDelay(): Number? {
        val lastSeenBlockTimestamp = lastBlock?.timestamp ?: return null
        val currentTimestamp = Instant.now().epochSecond
        return max(currentTimestamp - lastSeenBlockTimestamp, 0).toDouble()
    }

    fun getErrorBlockCount(): Double {
        return blocksWithErrorsStatus.toDouble()
    }

    fun getPendingBlockCount(): Double {
        return blocksWithPendingStatus.toDouble()
    }
}