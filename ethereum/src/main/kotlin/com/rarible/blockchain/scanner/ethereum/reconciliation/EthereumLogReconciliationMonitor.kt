package com.rarible.blockchain.scanner.ethereum.reconciliation

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.monitoring.AbstractMonitor
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class EthereumLogReconciliationMonitor(
    properties: EthereumScannerProperties,
    meterRegistry: MeterRegistry
) : AbstractMonitor(
    properties,
    meterRegistry,
    "log_reconciliation"
) {
    private val reconciledRangeCounter: Counter = addCounter(
        metricName = "reconciled_range"
    )
    private val inconsistencyCounter: Counter = addCounter(
        metricName = "inconsistency"
    )

    override fun register() = Unit

    fun onReconciledRange(size: Long) {
        reconciledRangeCounter.increment(size.toDouble())
    }

    fun onInconsistency() {
        inconsistencyCounter.increment()
    }
}
