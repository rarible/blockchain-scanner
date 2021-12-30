package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.NewStableBlockEvent
import com.rarible.blockchain.scanner.framework.data.NewUnstableBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.test.TestBlockchainScanner
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestBlockchainClient
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.publisher.TestLogRecordEventPublisher
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.subscriber.TestLogEventSubscriber
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

    protected val testLogRepository: TestLogRepository by lazy {
        TestLogRepository(mongo)
    }

    protected val testLogService: TestLogService by lazy {
        TestLogService(testLogRepository)
    }

    protected val testLogRecordEventPublisher: TestLogRecordEventPublisher = TestLogRecordEventPublisher()

    protected suspend fun findBlock(number: Long): Block? = blockRepository.findById(number)

    protected suspend fun findLog(collection: String, id: Long): TestLogRecord? {
        return testLogRepository.findLogEvent(TestCustomLogRecord::class.java, collection, id)
    }

    protected fun createBlockchainScanner(
        testBlockchainClient: TestBlockchainClient,
        vararg subscribers: TestLogEventSubscriber
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
            subscribers = subscribers.toList()
        )
    }

    protected suspend fun getAllBlocks(): List<Block> = blockRepository.getAll().toList().sortedBy { it.id }

    protected fun TestBlockchainBlock.asNewEvent() = NewUnstableBlockEvent(this)
    protected fun TestBlockchainBlock.asRevertEvent() = RevertedBlockEvent<TestBlockchainBlock>(this.number, this.hash)

}
