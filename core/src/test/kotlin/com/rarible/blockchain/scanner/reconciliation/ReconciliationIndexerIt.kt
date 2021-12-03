package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.event.log.LogEventHandler
import com.rarible.blockchain.scanner.publisher.LogEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.client.TestBlockchainLog
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.randomBlockchainData
import com.rarible.blockchain.scanner.test.model.TestBlock
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
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

@ExperimentalCoroutinesApi
@FlowPreview
@IntegrationTest
class ReconciliationIndexerIt : AbstractIntegrationTest() {

    @Autowired
    @Qualifier("testSubscriber1")
    lateinit var subscriber: TestLogEventSubscriber

    private var topic = ""
    private var collection = ""
    private var batchSize = -1L

    private val logEventPublisher: LogEventPublisher = mockk()

    @BeforeEach
    fun beforeEach() {
        clearMocks(logEventPublisher)
        coEvery { logEventPublisher.publish(any()) } returns Unit

        topic = subscriber.getDescriptor().topic
        collection = subscriber.getDescriptor().collection
        batchSize = properties.job.reconciliation.batchSize
    }

    @Test
    fun `reindex - all logs in storage`() = runBlocking {
        // We have blocks in DB, but there in no LogRecords
        val blockchainData = randomBlockchainData(13, 2, topic)

        val indexer = createReconciliationIndexer(TestBlockchainClient(blockchainData))
        val ranges = indexer.reindex(0, 11, batchSize).toList()

        // Batch size is 5, so we should have 3 ranges here
        assertEquals(3, ranges.size)

        // After reindex there should be 11*2 logs (2 per reindexed block)
        val logs = findAllLogs(collection)
        assertEquals(12 * 2, logs.size)

        // During reconciliation we are NOT saving block data/block status
        assertEquals(12, findAllBlocks().size)

        // Since 12 blocks processed, 12 BlockEvents should be published
        coVerify(exactly = 12) { logEventPublisher.publish(any()) }
    }

    @Test
    fun `reindex - some logs already in storage`() = runBlocking {
        val blockchainData = randomBlockchainData(9, 2, topic)

        // Reindexing part of logs
        val indexer = createReconciliationIndexer(TestBlockchainClient(blockchainData))
        indexer.reindex(0, 4, batchSize).collect()

        // After reindex there should be 5*2 logs (2 per reindexed block)
        val newLogs = findAllLogs(collection)
        assertEquals(5 * 2, newLogs.size)

        // Reindexing same blocks - no new LogRecords should be created
        indexer.reindex(0, 4, batchSize).collect()
        val sameReindexedLogs = findAllLogs(collection)
        assertEquals(newLogs.size, sameReindexedLogs.size)

        // Another reindex - records from new blocks should be added
        indexer.reindex(0, 7, batchSize).collect()
        val allReindexedLogs = findAllLogs(collection)
        assertEquals(8 * 2, allReindexedLogs.size)

        // Reconciliation job was launched 3 times, for 5/5/8 blocks, 18 events should be published
        coVerify(exactly = 5 + 5 + 8) { logEventPublisher.publish(any()) }
    }

    @Test
    fun `reindex - reindex range greater than existing max block number`() = runBlocking {
        val blockchainData = randomBlockchainData(2, 1, topic)

        val indexer = createReconciliationIndexer(TestBlockchainClient(blockchainData))
        indexer.reindex(0, 4, batchSize).collect()

        // Only logs from 2 existed blocks should be stored
        val newLogs = findAllLogs(collection)
        assertEquals(2, newLogs.size)

        // Events should be published only for existing blocks
        coVerify(exactly = 2) { logEventPublisher.publish(any()) }
    }

    private fun createReconciliationIndexer(
        testBlockchainClient: TestBlockchainClient
    ): ReconciliationIndexer<TestBlockchainBlock, TestBlock, TestBlockchainLog, TestLog, TestLogRecord<*>, TestDescriptor> {

        return ReconciliationIndexer(
            testBlockchainClient,
            LogEventHandler(subscriber, testLogMapper, testLogService),
            logEventPublisher, blockService = testBlockService, blockMapper = testBlockMapper
        )
    }

}
