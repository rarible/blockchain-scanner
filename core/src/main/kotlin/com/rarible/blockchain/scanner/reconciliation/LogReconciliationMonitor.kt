package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.monitoring.AbstractMonitor
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class LogReconciliationMonitor(
    properties: BlockchainScannerProperties,
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
