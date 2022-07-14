package com.rarible.blockchain.scanner.monitoring

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
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

    protected fun addTimer(metricName: String, vararg tags: Tag): Timer {
        return Timer.builder("$fullPrefix.$metricName")
            .tag("blockchain", blockchain)
            .tags(tags.toList())
            .register(meterRegistry)
    }

    protected fun addCounter(metricName: String, vararg tags: Tag): Counter {
        return Counter.builder("$fullPrefix.$metricName")
            .tag("blockchain", blockchain)
            .tags(tags.toList())
            .register(meterRegistry)
    }

    override fun toString(): String {
        return this.javaClass.name + "(" + properties.blockchain + ")"
    }
}
