package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.Source
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.*
import com.rarible.blockchain.scanner.test.mapper.TestLogMapper
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.service.TestPendingLogService
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

@IntegrationTest
class BlockEventHandlerIt {

    @Autowired
    lateinit var testLogMapper: TestLogMapper

    @Autowired
    lateinit var testLogService: TestLogService

    @Autowired
    lateinit var testPendingLogService: TestPendingLogService

    @Test
    fun `on block event - two subscribers`() = runBlocking {
        val subscriber1 = TestLogEventSubscriber(testDescriptor1())
        val subscriber2 = TestLogEventSubscriber(testDescriptor1())
        val block = randomOriginalBlock()
        val log = randomOriginalLog(block.hash, subscriber1.getDescriptor().topic)

        val testBlockchainClient = TestBlockchainClient(TestBlockchainData(listOf(block), listOf(log)))

        val blockEventHandler = createBlockHandler(testBlockchainClient, subscriber1, subscriber2)

        val event = BlockEvent(Source.BLOCKCHAIN, TestBlockchainBlock(block))
        val logEvents = blockEventHandler.onBlockEvent(event).toCollection(mutableListOf())

        assertEquals(2, logEvents.size)

        // Since we have two subscribers for same topic, we await 2 similar events here
        assertBlockchainLogAndLogEquals(log, logEvents[0])
        assertBlockchainLogAndLogEquals(log, logEvents[1])
    }

    private fun createBlockHandler(
        testBlockchainClient: TestBlockchainClient,
        vararg subscribers: TestLogEventSubscriber
    ): BlockEventHandler<TestBlockchainBlock, TestBlockchainLog, TestLog, TestDescriptor> {
        return BlockEventHandler(
            testBlockchainClient,
            subscribers.asList(),
            testLogMapper,
            testLogService,
            testPendingLogService
        )
    }

}