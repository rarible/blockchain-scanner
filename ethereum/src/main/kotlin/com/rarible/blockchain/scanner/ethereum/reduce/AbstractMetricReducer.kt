package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.core.entity.reducer.service.Reducer
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.ConcurrentHashMap

abstract class AbstractMetricReducer<Event, E>(
    private val metricProperties: MetricProperties,
    private val meterRegistry: MeterRegistry,
    prefix: String,
) : Reducer<Event, E> {

    private val blockchain = metricProperties.blockchain.value
    private val fullPrefix = "${metricProperties.metricRootPath}.reduce.$prefix"
    private val counters = ConcurrentHashMap<Class<out Event>, Counter>()

    override suspend fun reduce(entity: E, event: Event): E {
        counters.computeIfAbsent(requireNotNull(event)::class.java) {
            createCounter(getMetricName(event))
        }.increment()

        return entity
    }

    protected abstract fun getMetricName(event: Event): String

    private fun createCounter(metricName: String): Counter {
        return Counter.builder("$fullPrefix.$metricName")
            .tag("blockchain", blockchain)
            .register(meterRegistry)
    }

    override fun toString(): String {
        return this.javaClass.name + "(" + metricProperties.blockchain + ")"
    }
}
