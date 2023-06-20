package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.AsyncFlowAccessApi
import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Component
import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap

@Primary
@Component
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