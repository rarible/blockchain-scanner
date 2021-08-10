package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import java.util.function.Supplier

abstract class AbstractMonitor(
    private val properties: BlockchainScannerProperties,
    private val meterRegistry: MeterRegistry,
    prefix: String
) : Monitor {

    private val blockchain = properties.blockchain
    private val fullPrefix = properties.monitoring.rootPath + "." + prefix

    protected fun addGauge(metricName: String, supplier: Supplier<Number?>) {
        Gauge.builder("$fullPrefix.$metricName", supplier)
            .tag("blockchain", blockchain)
            .register(meterRegistry)
    }

    override fun toString(): String {
        return this.javaClass.name + "(" + properties.blockchain + ")"
    }
}