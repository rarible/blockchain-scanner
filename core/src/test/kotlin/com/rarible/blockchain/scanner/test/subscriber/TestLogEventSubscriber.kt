package com.rarible.blockchain.scanner.test.subscriber

import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import org.reactivestreams.Publisher
import reactor.kotlin.core.publisher.toFlux

class TestLogEventSubscriber(
    private val descriptor: LogEventDescriptor,
    private val eventDataCount: Int = 1
) : LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog> {

    override fun getDescriptor(): LogEventDescriptor {
        return descriptor
    }

    override fun getEventData(block: TestBlockchainBlock, log: TestBlockchainLog): Publisher<EventData> {
        val eventDataList = ArrayList<TestEventData>(eventDataCount)
        for (i in 0 until eventDataCount) {
            eventDataList.add(TestEventData(i, block.testOriginalBlock.testExtra, log.testOriginalLog.testExtra))
        }
        return eventDataList.toFlux()
    }
}