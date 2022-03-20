package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.model.Descriptor
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import java.util.concurrent.ConcurrentHashMap

class LogMonitor(
    properties: BlockchainScannerProperties,
    meterRegistry: MeterRegistry
) : AbstractMonitor(
    properties,
    meterRegistry,
    "log"
) {

    private val logCounters = ConcurrentHashMap<String, Counter>()

    override fun register() = Unit

    override fun refresh() = Unit

    private fun getInsertedLogsCounter(descriptor: Descriptor): Counter =
        logCounters.getOrPut(descriptor.id) {
            addCounter(
                metricName = "inserted_logs",
                Tag.of("subscriber", descriptor.id)
            )
        }

    fun onLogsInserted(descriptor: Descriptor, inserted: Int) {
        getInsertedLogsCounter(descriptor).increment(inserted.toDouble())
    }
}
