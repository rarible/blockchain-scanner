package com.rarible.blockchain.scanner.ethereum.metrics

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.monitoring.AbstractMonitor
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class ReindexTaskMetrics(
    properties: EthereumScannerProperties,
    meterRegistry: MeterRegistry,
) : AbstractMonitor(
    properties = properties,
    meterRegistry = meterRegistry,
    prefix = "task"
) {
    fun onReindex(from: Long, to: Long?, state: Double, name: String?) {
        set(
            REINDEX,
            state,
            tag("name", name ?: "unknown"),
            tag("from", from.toString()),
            tag("to", to?.toString() ?: "latest"),
        )
    }

    override fun register() = Unit

    override fun refresh() = Unit

    private companion object {
        const val REINDEX = "reindex"
    }
}