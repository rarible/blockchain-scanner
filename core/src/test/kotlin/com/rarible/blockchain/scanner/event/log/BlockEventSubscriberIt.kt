package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.subscriber.LogEventSubscriber
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.TestBlockchainData
import com.rarible.blockchain.scanner.test.data.randomBlockHash
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomBlockchainLog
import com.rarible.blockchain.scanner.test.data.randomOriginalBlock
import com.rarible.blockchain.scanner.test.data.randomOriginalLog
import com.rarible.blockchain.scanner.test.data.randomTestLogRecord
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@IntegrationTest
class BlockEventSubscriberIt : AbstractIntegrationTest() {

    private val descriptor = testDescriptor1()
    private val topic = descriptor.id
    private val collection = descriptor.collection

    @Test
    fun `on new block event`() = runBlocking<Unit> {
        val subscriber = TestLogEventSubscriber(descriptor)
        val originalBlock = randomOriginalBlock()
        val originalLog = randomOriginalLog(originalBlock.hash, topic)

        val testBlockchainClient = TestBlockchainClient(TestBlockchainData(listOf(originalBlock), listOf(originalLog)))
        val blockSubscriber = createBlockSubscriber(subscriber, testBlockchainClient)

        val event = NewBlockEvent(Source.BLOCKCHAIN, originalBlock.number, originalBlock.hash)
        val logEvents = blockSubscriber.onNewBlockEvents(listOf(event))
        assertThat(logEvents).isEqualTo(
            listOf(
                LogEvent(
                    blockEvent = event,
                    descriptor = descriptor,
                    logRecordsToInsert = subscriber.getReturnedRecords(originalBlock, originalLog),
                    logRecordsToRemove = emptyList()
                )
            )
        )
    }

    @Test
    fun `prepare logs saved with corrrect index and minor index`() = runBlocking {
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

    @Test
    fun `revert block`() = runBlocking<Unit> {
        val revertedBlock = randomBlockchainBlock()
        val revertedBlockEvent = RevertedBlockEvent(Source.BLOCKCHAIN, revertedBlock.number, revertedBlock.hash)

        val log1 = randomTestLogRecord(topic = topic, blockHash = revertedBlock.hash, status = Log.Status.CONFIRMED)
        val log2 = randomTestLogRecord(topic = topic, blockHash = revertedBlock.hash, status = Log.Status.CONFIRMED)
        val log3 = randomTestLogRecord(topic = topic, blockHash = randomBlockHash(), status = Log.Status.CONFIRMED)

        testLogRepository.saveAll(collection, log1, log2, log3)

        val blockSubscriber = createBlockSubscriber(subscriber = TestLogEventSubscriber(descriptor))
        val logEvents = blockSubscriber.onRevertedBlockEvents(listOf(revertedBlockEvent)).map {
            it.copy(logRecordsToRemove = it.logRecordsToRemove.sortedWith(testLogEventComparator))
        }
        assertThat(logEvents).isEqualTo(
            listOf(
                LogEvent(
                    blockEvent = revertedBlockEvent,
                    descriptor = descriptor,
                    logRecordsToInsert = emptyList(),
                    logRecordsToRemove = listOf(
                        log1.copy(version = 0).withLog(log1.log.copy(status = Log.Status.REVERTED)),
                        log2.copy(version = 0).withLog(log2.log.copy(status = Log.Status.REVERTED))
                    ).sortedWith(testLogEventComparator)
                )
            )
        )
    }

    private fun createBlockSubscriber(
        subscriber: LogEventSubscriber<TestBlockchainBlock, TestBlockchainLog, TestLog, TestLogRecord<*>, TestDescriptor>,
        testBlockchainClient: TestBlockchainClient = TestBlockchainClient(TestBlockchainData()),
    ) = BlockEventSubscriber(
        testBlockchainClient,
        subscriber,
        testLogService
    )
}
