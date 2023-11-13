package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import io.micrometer.core.instrument.MeterRegistry

class ReindexMonitor(
    properties: BlockchainScannerProperties,
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

    private companion object {
        const val REINDEX = "reindex"
    }
}
