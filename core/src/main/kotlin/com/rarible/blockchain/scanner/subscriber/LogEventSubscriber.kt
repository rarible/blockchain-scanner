package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.EventData
import org.reactivestreams.Publisher

interface LogEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog> {

    fun getDescriptor(): LogEventDescriptor

    //todo кажется тут Flow просто вернуть нужно
    fun getEventData(block: BB, log: BL): Publisher<EventData>

}