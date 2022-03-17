package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.test.TestBlockchainScanner
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.publisher.TestLogRecordEventPublisher
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventFilter
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.ReactiveMongoOperations

abstract class AbstractIntegrationTest {

    @Autowired
    protected lateinit var mongo: ReactiveMongoOperations

    private val blockRepository: BlockRepository by lazy {
        BlockRepository(mongo)
    }

    protected val blockService: BlockService by lazy {
        BlockService(blockRepository)
    }

    private val testLogRepository: TestLogRepository by lazy {
        TestLogRepository(mongo)
    }

    private val testLogService: TestLogService by lazy {
        TestLogService(testLogRepository)
    }

    protected val testLogRecordEventPublisher: TestLogRecordEventPublisher = TestLogRecordEventPublisher()

    protected suspend fun findBlock(number: Long): Block? = blockRepository.findById(number)

    protected suspend fun findLog(collection: String, id: Long): TestLogRecord? {
        return testLogRepository.findLogEvent(TestCustomLogRecord::class.java, collection, id)
    }

    protected fun createBlockchainScanner(
        testBlockchainClient: TestBlockchainClient,
        subscribers: List<TestLogEventSubscriber>,
        logFilters: List<TestLogEventFilter> = emptyList()
    ): TestBlockchainScanner {
        return TestBlockchainScanner(
            blockchainClient = testBlockchainClient,
            blockService = blockService,
            logService = testLogService,
            properties = TestBlockchainScannerProperties(
                retryPolicy = RetryPolicyProperties(
                    scan = ScanRetryPolicyProperties(
                        reconnectAttempts = 1
                    )
                )
            ),
            logRecordEventPublisher = testLogRecordEventPublisher,
            subscribers = subscribers.toList(),
            logFilters = logFilters,
            monitor = mockk(relaxed = true)
        )
    }

    protected suspend fun getAllBlocks(): List<Block> = blockRepository.getAll().toList().sortedBy { it.id }
}
