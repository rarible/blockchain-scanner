package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.LogEventDescriptor
import org.reactivestreams.Publisher

interface LogEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog, D : LogEventDescriptor> {

    fun getDescriptor(): D

    fun getEventData(block: BB, log: BL): Publisher<EventData>

}