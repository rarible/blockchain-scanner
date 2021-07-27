package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.model.EventData
import org.reactivestreams.Publisher

interface LogEventSubscriber<OL, OB, D : EventData> {

    fun getDescriptor(): LogEventDescriptor

    fun getEventData(log: OL, block: OB): Publisher<D>

}