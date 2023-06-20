package com.rarible.blockchain.scanner.monitoring

import com.rarible.core.telemetry.metrics.Metric.Companion.tag
import io.micrometer.core.instrument.MeterRegistry

class BlockchainMonitor(
    private val meterRegistry: MeterRegistry
) {
    private val blockchainApiCallsCounter = "blockchain_api_calls"

    fun onBlockchainCall(blockchain: String, method: String) {
        meterRegistry.counter(
            blockchainApiCallsCounter,
            listOf(tag("blockchain", blockchain), tag("method", method))
        ).increment()
    }
}