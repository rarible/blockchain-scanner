package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.event.log.BlockEventSubscriber
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.assertOriginalLogAndLogEquals
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.blockchain.scanner.test.data.randomOriginalLog
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@FlowPreview
@ExperimentalCoroutinesApi
@IntegrationTest
class BlockEventSubscriberIt : AbstractIntegrationTest() {

    private val descriptor = testDescriptor1()
    private val topic = descriptor.topic

    @Test
    fun `on block event`() = runBlocking {
        val subscriber = TestLogEventSubscriber(descriptor)
        val block = randomOriginalBlock()
        val log = randomOriginalLog(block.hash, topic)

        val testBlockchainClient = TestBlockchainClient(TestBlockchainData(listOf(block), listOf(log)))

        val blockSubscriber = createBlockSubscriber(testBlockchainClient, subscriber)

        val event = NewBlockEvent(Source.BLOCKCHAIN, block.number, block.hash)
        val logEvents = blockSubscriber.onNewBlockEvents(listOf(event)).values.flatten()

        // We are expecting here event from pending logs and then event from new block
        assertEquals(1, logEvents.size)

        assertEquals(Log.Status.CONFIRMED, logEvents[0].log!!.status)
        assertEquals(true, logEvents[0].log!!.visible)
        assertOriginalLogAndLogEquals(log, logEvents[0].log!!)
    }

    private fun createBlockSubscriber(
        testBlockchainClient: TestBlockchainClient,
        subscriber: TestLogEventSubscriber,
    ): BlockEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLog, TestLogRecord<*>, TestDescriptor> {
        return BlockEventSubscriber(
            testBlockchainClient,
            subscriber,
            testLogMapper,
            testLogService
        )
    }
}


