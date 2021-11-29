package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.subscriber.ProcessedBlockEvent
import com.rarible.blockchain.scanner.test.data.defaultTestProperties
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomTestLogRecord
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventListener
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

@ExperimentalCoroutinesApi
internal class LogEventPublisherTest {

    private val properties = defaultTestProperties().retryPolicy.scan

    @Test
    fun `on block processed - listeners notified`() = runBlocking {
        val listener1 = spyk(TestLogEventListener())
        val listener2 = spyk(TestLogEventListener())
        val publisher = LogEventPublisher(listOf(listener1, listener2), properties)

        val block = randomBlockchainBlock()
        val blockEvent = NewBlockEvent(Source.BLOCKCHAIN, block.number, block.hash)
        val events: List<TestLogRecord<*>> = listOf(randomTestLogRecord(), randomTestLogRecord())
        val processedBlockEvent = ProcessedBlockEvent(blockEvent, events)

        val status = publisher.onBlockProcessed(blockEvent, events)

        assertEquals(Block.Status.SUCCESS, status)
        coVerify(exactly = 1) { listener1.onBlockLogsProcessed(processedBlockEvent) }
        coVerify(exactly = 1) { listener2.onBlockLogsProcessed(processedBlockEvent) }
    }

    @Test
    fun `on block processed - listener failed`() = runBlocking {
        val listener1 = spyk(TestLogEventListener())
        val listener2: TestLogEventListener = mockk()
        coEvery { listener2.onBlockLogsProcessed(any()) }.throws(IllegalArgumentException())
        val listener3 = spyk(TestLogEventListener())
        val publisher = LogEventPublisher(listOf(listener1, listener2, listener3), properties)

        val block = randomBlockchainBlock()
        val blockEvent = NewBlockEvent(Source.BLOCKCHAIN, block.number, block.hash)
        val events: List<TestLogRecord<*>> = listOf(randomTestLogRecord(), randomTestLogRecord())
        val processedBlockEvent = ProcessedBlockEvent(blockEvent, events)

        val status = publisher.onBlockProcessed(blockEvent, events)

        assertEquals(Block.Status.ERROR, status)
        coVerify(exactly = 1) { listener1.onBlockLogsProcessed(processedBlockEvent) }
        coVerify(exactly = 1) { listener2.onBlockLogsProcessed(processedBlockEvent) }
        coVerify(exactly = 0) { listener3.onBlockLogsProcessed(processedBlockEvent) }
    }

    @Test
    fun `on block processed - timeout`() = runBlocking {
        val strictProperties = properties.copy(maxProcessTime = Duration.ofMillis(100))
        val slowListener: TestLogEventListener = mockk()
        coEvery { slowListener.onBlockLogsProcessed(any()) }.coAnswers {
            delay(strictProperties.maxProcessTime.toMillis() + 10)
        }

        val publisher = LogEventPublisher(listOf(slowListener), strictProperties)

        val block = randomBlockchainBlock()
        val blockEvent = NewBlockEvent(Source.BLOCKCHAIN, block.number, block.hash)
        val events: List<TestLogRecord<*>> = listOf(randomTestLogRecord())
        val processedBlockEvent = ProcessedBlockEvent(blockEvent, events)

        val status = publisher.onBlockProcessed(blockEvent, events)

        assertEquals(Block.Status.ERROR, status)
        coVerify(exactly = 1) { slowListener.onBlockLogsProcessed(processedBlockEvent) }
    }

}
