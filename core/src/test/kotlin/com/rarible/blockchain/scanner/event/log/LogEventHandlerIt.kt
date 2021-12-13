package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.assertRecordAndLogEquals
import com.rarible.blockchain.scanner.test.data.randomBlockHash
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomBlockchainLog
import com.rarible.blockchain.scanner.test.data.randomTestLogRecord
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

@ExperimentalCoroutinesApi
@FlowPreview
@IntegrationTest
internal class LogEventHandlerIt : AbstractIntegrationTest() {

    private val descriptor = testDescriptor1()
    private val collection = descriptor.collection
    private val topic = descriptor.id

    @Test
    fun `handle logs - log saved with minor index`() = runBlocking {
        val handler = createHandler(TestLogEventSubscriber(descriptor, 2))
        val block = randomBlockchainBlock()
        val log = randomBlockchainLog(block, topic)

        val savedLogs = handler.handleLogs(FullBlock(block, listOf(log))).toCollection(mutableListOf())

        assertEquals(2, savedLogs.size)

        val savedLog1 = findLog(collection, savedLogs[0].id)!!
        val savedLog2 = findLog(collection, savedLogs[1].id)!!

        // These event logs are based on same Blockchain log, so basic params should be the same
        assertTrue(savedLog1 is TestCustomLogRecord)
        assertTrue(savedLog1 is TestCustomLogRecord)
        assertRecordAndLogEquals(savedLog1, log.testOriginalLog, block.testOriginalBlock)
        assertRecordAndLogEquals(savedLog2, log.testOriginalLog, block.testOriginalBlock)
        assertEquals(0, savedLog1.log.index)
        assertEquals(0, savedLog2.log.index)

        // Minor index should be different because there are 2 custom data objects generated for single Blockchain Log
        assertEquals(0, savedLog1.log.minorLogIndex)
        assertEquals(1, savedLog2.log.minorLogIndex)
    }

    @Test
    fun `handle logs - logs saved with index and minor index`() = runBlocking {
        val handler = createHandler(TestLogEventSubscriber(testDescriptor1(), 3))
        val block = randomBlockchainBlock()
        val logs = listOf(randomBlockchainLog(block, topic), randomBlockchainLog(block, topic))

        val savedLogs = handler.handleLogs(FullBlock(block, logs)).toCollection(mutableListOf())

        assertEquals(6, savedLogs.size)

        val fromDb = savedLogs.map { findLog(collection, it.id)!! }
        val indices = fromDb.map { it.log.index }
        val minorIndices = fromDb.map { it.log.minorLogIndex }

        assertIterableEquals(listOf(0, 0, 0, 1, 1, 1), indices)
        assertIterableEquals(listOf(0, 1, 2, 0, 1, 2), minorIndices)
    }

    @Test
    fun `handle logs - nothing on empty flow`() = runBlocking {
        val handler = createHandler(TestLogEventSubscriber(descriptor, 3))
        val block = randomBlockchainBlock()

        val savedLogs = handler.handleLogs(FullBlock(block, emptyList())).toCollection(mutableListOf())
        assertEquals(0, savedLogs.size)
    }

    @Test
    fun `revert block`() = runBlocking {
        val handler = createHandler(TestLogEventSubscriber(descriptor))

        val reverted = randomBlockchainBlock()
        val event = RevertedBlockEvent(Source.BLOCKCHAIN, reverted.number, reverted.hash)

        val log1 = randomTestLogRecord(topic, reverted.hash, Log.Status.REVERTED)
        val log2 = randomTestLogRecord(topic, reverted.hash, Log.Status.CONFIRMED)
        val log3 = randomTestLogRecord(topic, randomBlockHash(), Log.Status.REVERTED)

        testLogRepository.saveAll(collection, log1, log2, log3)

        val revertedLogs = handler.revert(event).toCollection(mutableListOf())
        assertEquals(2, revertedLogs.size)
        val log1Event = revertedLogs.find { it.id == log1.id }!!
        val log2Event = revertedLogs.find { it.id == log2.id }!!

        val log1FromDb = findLog(collection, log1.id)
        val log2FromDb = findLog(collection, log2.id)
        val log3FromDb = findLog(collection, log3.id)!!

        // Log 1 and 2 should be deleted and published with status REVERTED
        assertNull(log1FromDb)
        assertEquals(log1.id, log1Event.id)
        assertEquals(Log.Status.REVERTED, log1Event.log.status)

        assertNull(log2FromDb)
        assertEquals(log2.id, log2Event.id)
        assertEquals(Log.Status.REVERTED, log2Event.log.status)

        // This log should not be changed since it related to another block
        assertEquals(log3.log.status, log3FromDb.log.status)
    }

    private fun createHandler(
        subscriber: TestLogEventSubscriber
    ): LogEventHandler<TestBlockchainBlock, TestBlockchainLog, TestLog, TestLogRecord<*>, TestDescriptor> {
        return LogEventHandler(
            subscriber,
            testLogMapper,
            testLogService
        )
    }

}
