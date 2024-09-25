package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.TimestampUnit
import com.rarible.blockchain.scanner.monitoring.BlockMonitor
import com.rarible.blockchain.scanner.monitoring.LogMonitor
import com.rarible.blockchain.scanner.test.TestBlockchainScanner
import com.rarible.blockchain.scanner.test.TestBlockchainScannerManager
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.publisher.TestLogRecordEventPublisher
import com.rarible.blockchain.scanner.test.publisher.TestTransactionRecordEventPublisher
import com.rarible.blockchain.scanner.test.repository.TestLogStorage
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriberExceptionResolver
import com.rarible.blockchain.scanner.test.subscriber.TestTransactionEventSubscriber
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.convert.MongoConverter

abstract class AbstractIntegrationTest {

    @Autowired
    protected lateinit var mongo: ReactiveMongoOperations

    @Autowired
    protected lateinit var mongoConverter: MongoConverter

    private val blockRepository: BlockRepository by lazy {
        BlockRepository(mongo, mongoConverter)
    }

    protected val blockService: BlockService by lazy {
        BlockService(blockRepository)
    }

    private val testLogService: TestLogService by lazy {
        TestLogService()
    }

    protected val testLogRecordEventPublisher: TestLogRecordEventPublisher = TestLogRecordEventPublisher()
    protected val testTransactionRecordEventPublisher: TestTransactionRecordEventPublisher =
        TestTransactionRecordEventPublisher()

    protected suspend fun findBlock(number: Long): Block? = blockRepository.findById(number)

    protected fun createBlockchainScanner(
        testBlockchainClient: TestBlockchainClient,
        subscribers: List<TestLogEventSubscriber>,
        transactionSubscribers: List<TestTransactionEventSubscriber>,
        scanProperties: ScanProperties = ScanProperties(),
        scanRetryProperties: ScanRetryPolicyProperties = ScanRetryPolicyProperties(reconnectAttempts = 1)
    ): TestBlockchainScanner {
        val manager = TestBlockchainScannerManager(
            blockchainClient = testBlockchainClient,
            blockService = blockService,
            logService = testLogService,
            properties = TestBlockchainScannerProperties(
                scan = scanProperties,
                retryPolicy = RetryPolicyProperties(
                    scan = scanRetryProperties
                )
            ),
            logRecordEventPublisher = testLogRecordEventPublisher,
            subscribers = subscribers.toList(),
            blockMonitor = createBlockMonitor(),
            logMonitor = createLogMonitor(),
            reindexMonitor = mockk(relaxed = true),
            transactionRecordEventPublisher = testTransactionRecordEventPublisher,
            transactionSubscribers = transactionSubscribers,
            logEventSubscriberExceptionResolver = TestLogEventSubscriberExceptionResolver()
        )
        return TestBlockchainScanner(manager)
    }

    private fun createBlockMonitor(): BlockMonitor {
        val monitor = BlockMonitor(
            blockRepository,
            createBlockchainScannerProperties(),
            SimpleMeterRegistry()
        )
        monitor.register()
        return monitor
    }

    private fun createLogMonitor(): LogMonitor {
        val monitor = LogMonitor(
            createBlockchainScannerProperties(),
            SimpleMeterRegistry()
        )
        monitor.register()
        return monitor
    }

    private fun createBlockchainScannerProperties(): BlockchainScannerProperties {
        return mockk<BlockchainScannerProperties> {
            every { monitoring.enabled } returns true
            every { monitoring.rootPath } returns "test"
            every { monitoring.timestampUnit } returns TimestampUnit.SECOND
            every { blockchain } returns "ethereum"
        }
    }

    protected suspend fun getAllBlocks(): List<Block> =
        blockRepository.getAll().toList().sortedBy { it.id }.map { it.copy(stats = null) }

    protected fun testDescriptor(
        topic: String,
        collection: String,
        groupId: String = topic,
        contracts: List<String> = emptyList(),
        entityType: Class<out TestLogRecord> = TestCustomLogRecord::class.java
    ) = TestDescriptor(
        topic = topic,
        collection = collection,
        contracts = contracts,
        entityType = entityType,
        groupId = groupId,
        storage = TestLogStorage(mongo, collection = collection, entityType = entityType),
    )
}
