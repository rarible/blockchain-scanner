package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.event.log.BlockEventListener
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
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

@ExperimentalCoroutinesApi
@FlowPreview
@IntegrationTest
class ReconciliationServiceIt : AbstractIntegrationTest() {

    @Autowired
    @Qualifier("testSubscriber1")
    lateinit var subscriber1: TestLogEventSubscriber

    @Autowired
    @Qualifier("testSubscriber2")
    lateinit var subscriber2: TestLogEventSubscriber

    private var topic1 = ""
    private var collection1 = ""
    private var topic2 = ""
    private var collection2 = ""
    private var logEventPublisher: LogEventPublisher = mockk()
    private var batchSize = -1L

    @BeforeEach
    fun beforeEach() {
        clearMocks(logEventPublisher)
        coEvery { logEventPublisher.publish(any(), any(), any()) } returns Unit

        batchSize = properties.job.reconciliation.batchSize
        topic1 = subscriber1.getDescriptor().id
        collection1 = subscriber1.getDescriptor().collection
        topic2 = subscriber2.getDescriptor().id
        collection2 = subscriber2.getDescriptor().collection
    }

    @Test
    fun `reindex - 2 subscribers reindexed`() = runBlocking {
        val blockchainData = randomBlockchainData(7, 2, topic1, topic2)
        blockchainData.blocks.forEach { saveBlock(it) }

        val reconciliationService = createReconciliationService(TestBlockchainClient(blockchainData))

        reconciliationService.reindex(topic1, 0, batchSize).toList()

        // Only first subscriber should be reindexed at this moment
        assertEquals(7 * 2, findAllLogs(collection1).size)
        assertEquals(0, findAllLogs(collection2).size)

        reconciliationService.reindex(topic2, 3, batchSize).toList().toList()

        // LogRecords for first subscriber should be the same,
        // LogRecord amount of second should be 16 (2 logs per 4 indexed blocks * 2 LogRecord per log from subscribers)
        assertEquals(7 * 2, findAllLogs(collection1).size)
        assertEquals(4 * 2 * 2, findAllLogs(collection2).size)

        // In total, we should have 7 + 4 published events
        coVerify(exactly = 11) { logEventPublisher.publish(any(), any(), any()) }
    }

    @Test
    fun `reindex - start block number is too big`() = runBlocking {
        val blockchainData = randomBlockchainData(1, 1, topic1)
        val reconciliationService = createReconciliationService(TestBlockchainClient(blockchainData))

        reconciliationService.reindex(topic1, 4, batchSize).toList()

        // Nothing should be reindexed
        assertEquals(0, findAllLogs(collection1).size)
        coVerify(exactly = 0) { logEventPublisher.publish(any(), any(), any()) }
    }

    @Test
    fun `reindex - descriptor not found`() = runBlocking<Unit> {
        val blockchainData = randomBlockchainData(1, 1, topic1)
        val reconciliationService = createReconciliationService(TestBlockchainClient(blockchainData))

        assertThrows<IllegalArgumentException> {
            runBlocking { reconciliationService.reindex("not exist", 0, batchSize).collect() }
        }
    }

    private fun createReconciliationService(
        testBlockchainClient: TestBlockchainClient
    ): ReconciliationService<TestBlockchainBlock, TestBlock, TestBlockchainLog, TestLog, TestLogRecord<*>, TestDescriptor> {
        val blockEventListeners = listOf(subscriber1, subscriber2)
            .groupBy { it.getDescriptor().groupId }
            .map {
                it.key to BlockEventListener(
                    testBlockchainClient,
                    it.value,
                    testLogService,
                    testLogEventComparator,
                    logEventPublisher
                )
            }.associateBy({ it.first }, { it.second })

        return ReconciliationService(
            testBlockService,
            blockEventListeners
        )
    }

}
