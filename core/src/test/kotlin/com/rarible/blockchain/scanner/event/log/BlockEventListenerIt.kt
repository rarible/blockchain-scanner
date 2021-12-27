package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.publisher.LogRecordEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.randomBlockHash
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomBlockchainLog
import com.rarible.blockchain.scanner.test.data.randomOriginalLog
import com.rarible.blockchain.scanner.test.data.randomTestLogRecord
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import com.rarible.blockchain.scanner.test.subscriber.TestLogRecordComparator
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@IntegrationTest
class BlockEventListenerIt : AbstractIntegrationTest() {

    private val descriptor = testDescriptor1()
    private val topic = descriptor.id
    private val collection = descriptor.collection

    private val publishedEvents = arrayListOf<LogRecordEvent<*>>()

    private val logRecordEventPublisher = object : LogRecordEventPublisher {
        override suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent<*>>) {
            publishedEvents += logRecordEvents
        }
    }

    @Test
    fun `on new block event`() = runBlocking<Unit> {
        val subscriber = TestLogEventSubscriber(descriptor)
        val block1 = randomBlockchainBlock()
        val block2 = randomBlockchainBlock()

        val log1 = randomOriginalLog(block1.hash, topic)
        val log2 = randomOriginalLog(block2.hash, topic)

        saveBlock(block1)
        saveBlock(block2)

        val testBlockchainClient = TestBlockchainClient(
            TestBlockchainData(
                blocks = listOf(block1, block2),
                logs = listOf(log1, log2)
            )
        )

        val blockEventListener = createBlockEventListener(testBlockchainClient, subscriber)

        val event1 = NewBlockEvent(block1.number, block1.hash)
        val event2 = NewBlockEvent(block2.number, block2.hash)

        // Explicitly reversed order of events - to check the publishing order.
        blockEventListener.onBlockEvents(listOf(event2, event1))

        assertThat(publishedEvents).isEqualTo(
            listOf(
                LogRecordEvent(
                    record = subscriber.getReturnedRecords(block2, log2).single(),
                    reverted = false
                ),
                LogRecordEvent(
                    record = subscriber.getReturnedRecords(block1, log1).single(),
                    reverted = false
                )
            )
        )
    }

    @Test
    fun `on reverted block event`() = runBlocking<Unit> {
        val revertedBlock = randomBlockchainBlock()
        val revertedBlockEvent = RevertedBlockEvent(revertedBlock.number, revertedBlock.hash)

        val log1 = randomTestLogRecord(topic = topic, blockHash = revertedBlock.hash)
        val log2 = randomTestLogRecord(topic = topic, blockHash = revertedBlock.hash)
        val log3 = randomTestLogRecord(topic = topic, blockHash = randomBlockHash())

        testLogRepository.saveAll(collection, log1, log2, log3)

        val subscriber = TestLogEventSubscriber(descriptor)
        val eventListener = createBlockEventListener(TestBlockchainClient(TestBlockchainData()), subscriber)
        eventListener.onBlockEvents(listOf(revertedBlockEvent))

        assertThat(publishedEvents).isEqualTo(
            listOf(
                LogRecordEvent(
                    record = log1.copy(version = 0),
                    reverted = true
                ),
                LogRecordEvent(
                    record = log2.copy(version = 0),
                    reverted = true
                )
            ).sortedWith(compareBy(TestLogRecordComparator) { it.record })
        )
    }

    @Test
    fun `process block events - two blocks passed to two subscribers`() = runBlocking<Unit> {
        val descriptor = testDescriptor1()
        val subscriber1 = TestLogEventSubscriber(descriptor)
        val subscriber2 = TestLogEventSubscriber(descriptor)

        val block1 = randomBlockchainBlock()
        val block2 = randomBlockchainBlock()

        val log1 = randomOriginalLog(block1.hash, descriptor.id)
        val log2 = randomOriginalLog(block2.hash, descriptor.id)

        val testBlockchainClient = TestBlockchainClient(TestBlockchainData(listOf(block1, block2), listOf(log1, log2)))

        val blockEventProcessor = createBlockEventListener(
            testBlockchainClient,
            subscriber1, subscriber2
        )

        val event1 = NewBlockEvent(block1.number, block1.hash)
        val event2 = NewBlockEvent(block2.number, block2.hash)
        val logEvents = blockEventProcessor.prepareBlockEvents(listOf(event1, event2))
        assertThat(logEvents).isEqualTo(
            listOf(
                LogEvent(
                    blockEvent = event1,
                    descriptor = descriptor,
                    logRecordsToInsert = subscriber1.getReturnedRecords(block1, log1),
                    logRecordsToRemove = emptyList()
                ),
                LogEvent(
                    blockEvent = event2,
                    descriptor = descriptor,
                    logRecordsToInsert = subscriber1.getReturnedRecords(block2, log2),
                    logRecordsToRemove = emptyList()
                ),
                LogEvent(
                    blockEvent = event1,
                    descriptor = descriptor,
                    logRecordsToInsert = subscriber2.getReturnedRecords(block1, log1),
                    logRecordsToRemove = emptyList()
                ),
                LogEvent(
                    blockEvent = event2,
                    descriptor = descriptor,
                    logRecordsToInsert = subscriber2.getReturnedRecords(block2, log2),
                    logRecordsToRemove = emptyList()
                )
            )
        )
    }

    @Test
    fun `prepare logs saved with correct index and minor index`() = runBlocking<Unit> {
        val blockEventSubscriber = createBlockSubscriber(TestLogEventSubscriber(testDescriptor1(), 3))
        val block = randomBlockchainBlock()
        val logs = listOf(
            randomBlockchainLog(block, topic, index = 5),
            randomBlockchainLog(block, topic, index = 7)
        )

        val savedLogs = blockEventSubscriber.prepareLogsToInsert(FullBlock(block, logs)).toCollection(mutableListOf())
        assertThat(savedLogs).hasSize(6)

        val indices = savedLogs.map { it.log.index }
        val minorIndices = savedLogs.map { it.log.minorLogIndex }

        Assertions.assertIterableEquals(listOf(5, 5, 5, 7, 7, 7), indices)
        Assertions.assertIterableEquals(listOf(0, 1, 2, 0, 1, 2), minorIndices)
    }

    private fun createBlockSubscriber(
        subscriber: LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLogRecord, TestDescriptor>,
        testBlockchainClient: TestBlockchainClient = TestBlockchainClient(TestBlockchainData()),
    ) = BlockEventSubscriber(
        testBlockchainClient,
        subscriber,
        testLogService
    )

    private fun createBlockEventListener(
        testBlockchainClient: TestBlockchainClient,
        vararg subscribers: TestLogEventSubscriber
    ) = BlockEventListener(
        testBlockchainClient,
        subscribers.toList(),
        testLogService,
        TestLogRecordComparator,
        logRecordEventPublisher
    )
}
