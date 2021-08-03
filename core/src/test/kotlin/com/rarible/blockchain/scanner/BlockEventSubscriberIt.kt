package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.Source
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.pending.PendingLogMarker
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.*
import com.rarible.blockchain.scanner.test.mapper.TestLogMapper
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

@IntegrationTest
class BlockEventSubscriberIt {

    @Autowired
    lateinit var testLogMapper: TestLogMapper

    @Autowired
    lateinit var testLogService: TestLogService

    private val descriptor = testDescriptor1()
    private val topic = descriptor.topic

    @Test
    fun `on block event`() = runBlocking {
        val subscriber = TestLogEventSubscriber(testDescriptor1())
        val block = randomOriginalBlock()
        val log = randomOriginalLog(block.hash, topic)
        val pendingLog = randomTestLogRecord(topic, block.hash)
        pendingLog.log = pendingLog.log!!.copy(status = Log.Status.PENDING)

        val testBlockchainClient = TestBlockchainClient(TestBlockchainData(listOf(block), listOf(log)))
        val pendingLogMarker = mockk<PendingLogMarker<TestBlockchainBlock, TestLog, TestLogRecord, TestDescriptor>>()

        coEvery {
            pendingLogMarker.markInactive(TestBlockchainBlock(block), subscriber.getDescriptor())
        } returns listOf(pendingLog).asFlow()

        val blockSubscriber = BlockEventSubscriber(
            testBlockchainClient,
            subscriber,
            testLogMapper,
            testLogService,
            pendingLogMarker
        )

        val event = BlockEvent(Source.BLOCKCHAIN, TestBlockchainBlock(block))
        val logEvents = blockSubscriber.onBlockEvent(event).toCollection(mutableListOf())

        // We are expecting here event from pending logs and then event from new block
        assertEquals(2, logEvents.size)

        assertEquals(pendingLog, logEvents[0])
        assertOriginalLogAndLogEquals(log, logEvents[1].log!!)
    }
}


