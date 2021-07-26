package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.model.EventData
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono

interface LogEventSubscriber<OL, OB, D : EventData> {

    val collection: String

    val topic: String

    fun map(log: OL, block: OB): Publisher<D>

    fun getAddresses(): Mono<Collection<String>>

    fun getDescriptor(): LogEventDescriptor {
        return LogEventDescriptor(
            collection, topic, getAddresses().block()!!
        )
    }
}