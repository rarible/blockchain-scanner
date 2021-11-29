package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.assertOriginalLogAndLogEquals
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.blockchain.scanner.test.data.randomOriginalLog
import com.rarible.blockchain.scanner.test.data.randomTestLogRecord
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import io.mockk.coEvery
import io.mockk.mockk
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
        val subscriber = TestLogEventSubscriber(testDescriptor1())
        val block = randomOriginalBlock()
        val log = randomOriginalLog(block.hash, topic)
        val pendingRecord = randomTestLogRecord(topic, block.hash, Log.Status.PENDING)

        val testBlockchainClient = TestBlockchainClient(TestBlockchainData(listOf(block), listOf(log)))

        val pendingLogMarker = mockk<PendingLogMarker<TestBlockchainBlock, TestLog, TestLogRecord<*>, TestDescriptor>>()
        coEvery {
            pendingLogMarker.markInactive(TestBlockchainBlock(block), subscriber.getDescriptor())
        } returns listOf(pendingRecord)

        val blockSubscriber = createBlockSubscriber(testBlockchainClient, subscriber, pendingLogMarker)

        val event = NewBlockEvent(Source.BLOCKCHAIN, block.number, block.hash)
        val logEvents = blockSubscriber.onBlockEvent(event).toCollection(mutableListOf())

        // We are expecting here event from pending logs and then event from new block
        assertEquals(2, logEvents.size)

        assertEquals(pendingRecord, logEvents[0])
        assertOriginalLogAndLogEquals(log, logEvents[1].log!!)
    }

    private fun createBlockSubscriber(
        testBlockchainClient: TestBlockchainClient,
        subscriber: TestLogEventSubscriber,
        pendingLogMarker: PendingLogMarker<TestBlockchainBlock, TestLog, TestLogRecord<*>, TestDescriptor>
    ): BlockEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLog, TestLogRecord<*>, TestDescriptor> {
        return BlockEventSubscriber(
            testBlockchainClient,
            subscriber,
            testLogMapper,
            testLogService,
            pendingLogMarker
        )
    }
}


