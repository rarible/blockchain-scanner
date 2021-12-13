package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.assertOriginalBlockAndBlockEquals
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomOriginalLog
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@ExperimentalCoroutinesApi
@FlowPreview
@IntegrationTest
internal class BlockEventListenerIt : AbstractIntegrationTest() {

    private val descriptor = testDescriptor1()
    private val topic = descriptor.id

    private val logEventPublisher: LogEventPublisher = mockk()

    @BeforeEach
    fun beforeEach() {
        clearMocks(logEventPublisher)
        coEvery { logEventPublisher.publish(any()) } returns Unit
    }

    @Test
    fun `on block event - event processed`() = runBlocking {
        val subscriber = TestLogEventSubscriber(descriptor)
        val block = randomBlockchainBlock()
        val log = randomOriginalLog(block.hash, topic)
        // Before log handling we have this block with default status = PENDING
        saveBlock(block.testOriginalBlock)

        val testBlockchainClient =
            TestBlockchainClient(TestBlockchainData(listOf(block.testOriginalBlock), listOf(log)))
        val blockEventListener = createBlockListener(testBlockchainClient, logEventPublisher, subscriber)

        val event = NewBlockEvent(Source.BLOCKCHAIN, block.number, block.hash)

        blockEventListener.onBlockEvents(listOf(event))

        // LogEvents processed, publisher notified listeners
        coVerify(exactly = 1) {
            logEventPublisher.publish(match {
                assertEquals(1, it.logRecords.size)
                assertEquals(log.transactionHash, it.logRecords[0].log.transactionHash)
                true
            })
        }

        // Block now have PROCESSED status, nothing else changed
        val savedBlock = testBlockRepository.findById(block.number)
        assertOriginalBlockAndBlockEquals(block.testOriginalBlock, savedBlock!!)
    }

    private fun createBlockListener(
        testBlockchainClient: TestBlockchainClient,
        testLogEventPublisher: LogEventPublisher,
        vararg subscribers: TestLogEventSubscriber
    ): BlockEventListener<TestBlockchainBlock, TestBlockchainLog, TestLog, TestLogRecord<*>, TestDescriptor> {
        return BlockEventListener(
            testBlockchainClient,
            subscribers.asList(),
            testLogMapper,
            testLogService,
            descriptor.groupId,
            testLogEventComparator,
            testLogEventPublisher
        )
    }
}
