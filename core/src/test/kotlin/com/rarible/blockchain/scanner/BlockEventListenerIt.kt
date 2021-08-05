package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.Source
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.*
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventListener
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import io.mockk.coVerify
import io.mockk.spyk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@IntegrationTest
internal class BlockEventListenerIt : AbstractIntegrationTest() {

    private val descriptor = testDescriptor1()
    private val topic = descriptor.topic

    @Test
    fun `on block event - event processed`() = runBlocking {
        val subscriber = TestLogEventSubscriber(descriptor)
        val block = randomBlockchainBlock()
        val log = randomOriginalLog(block.hash, topic)
        // Before log handling we have this block with default status = PENDING
        saveBlock(block.testOriginalBlock)

        val publisher = spyk(LogEventPublisher(listOf<TestLogEventListener>(), properties))
        val testBlockchainClient =
            TestBlockchainClient(TestBlockchainData(listOf(block.testOriginalBlock), listOf(log)))
        val blockEventListener = createBlockListener(testBlockchainClient, publisher, subscriber)

        val event = BlockEvent(Source.BLOCKCHAIN, block)

        blockEventListener.onBlockEvent(event)

        // LogEvents processed, publisher notified listeners
        coVerify(exactly = 1) {
            publisher.onBlockProcessed(eq(event), match {
                assertEquals(1, it.size)
                assertEquals(log.transactionHash, it[0].log!!.transactionHash)
                true
            })
        }

        // Block now have PROCESSED status, nothing else changed
        val savedBlock = testBlockRepository.findById(block.number)
        assertOriginalBlockAndBlockEquals(block.testOriginalBlock, savedBlock!!)
        assertEquals(Block.Status.SUCCESS, savedBlock.status)
    }

    private fun createBlockListener(
        testBlockchainClient: TestBlockchainClient,
        testLogEventPublisher: LogEventPublisher<TestLog, TestLogRecord<*>>,
        vararg subscribers: TestLogEventSubscriber
    ): BlockEventListener<TestBlockchainBlock, TestBlockchainLog, TestBlock, TestLog, TestLogRecord<*>, TestDescriptor> {
        return BlockEventListener(
            testBlockchainClient,
            subscribers.asList(),
            testBlockService,
            testLogMapper,
            testLogService,
            testPendingLogService,
            testLogEventPublisher
        )
    }
}