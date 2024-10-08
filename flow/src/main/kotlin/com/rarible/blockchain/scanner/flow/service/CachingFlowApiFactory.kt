package com.rarible.blockchain.scanner.flow.service

import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap

class CachingFlowApiFactory(
    private val flowApiFactory: FlowApiFactory
) : FlowApiFactory, Closeable {
    private val cache: MutableMap<Spork, AsyncFlowAccessApi> = ConcurrentHashMap<Spork, AsyncFlowAccessApi>()

    override fun getApi(spork: Spork): AsyncFlowAccessApi = cache.computeIfAbsent(spork) {
        flowApiFactory.getApi(it)
    }

    override fun close() {
        cache.values.filterIsInstance(Closeable::class.java)
            .forEach { it.close() }
    }
}
