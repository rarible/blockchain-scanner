package com.rarible.blockchain.scanner.flow.monitoring

import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.monitoring.AbstractMonitor
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class FlowMonitor(
    private val properties: FlowBlockchainScannerProperties,
    meterRegistry: MeterRegistry
) : AbstractMonitor(
    properties = properties,
    meterRegistry = meterRegistry,
    prefix = "flow"
) {
    private val blockchainNodeCallsCounter = "${properties.monitoring.rootPath}.flow.blockchain_node_calls"

    override fun refresh() = Unit

    override fun register() = Unit

    fun onBlockchainCall(method: String) {
        increment(blockchainNodeCallsCounter, tag("blockchain", properties.blockchain), tag("method", method))
    }
}