package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.BlockScanner
import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.mapper.TestBlockMapper
import com.rarible.blockchain.scanner.test.mapper.TestLogMapper
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.repository.TestBlockRepository
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import com.rarible.blockchain.scanner.test.service.TestBlockService
import com.rarible.blockchain.scanner.test.service.TestLogService
import com.rarible.blockchain.scanner.test.service.TestPendingLogService
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.findAll

@ExperimentalCoroutinesApi
@FlowPreview
abstract class AbstractIntegrationTest {

    @Autowired
    protected lateinit var mongo: ReactiveMongoOperations

    @Autowired
    lateinit var testBlockMapper: TestBlockMapper

    @Autowired
    lateinit var testBlockRepository: TestBlockRepository

    @Autowired
    lateinit var testBlockService: TestBlockService

    @Autowired
    lateinit var testLogMapper: TestLogMapper

    @Autowired
    lateinit var testLogRepository: TestLogRepository

    @Autowired
    lateinit var testLogService: TestLogService

    @Autowired
    lateinit var testPendingLogService: TestPendingLogService

    @Autowired
    lateinit var properties: TestBlockchainScannerProperties

    protected suspend fun findLog(collection: String, id: Long): TestLogRecord<*>? {
        return testLogRepository.findLogEvent(collection, id).awaitFirstOrNull()
    }

    protected suspend fun findBlock(number: Long): TestBlock? {
        return testBlockRepository.findById(number)
    }

    protected suspend fun findAllLogs(collection: String): List<Any> {
        return mongo.findAll<Any>(collection).collectList().awaitFirst()
    }

    protected suspend fun findAllBlocks(): List<TestBlock> {
        return mongo.findAll<TestBlock>().collectList().awaitFirst()
    }

    protected suspend fun saveBlock(
        block: TestOriginalBlock,
        status: Block.Status = Block.Status.SUCCESS
    ): TestOriginalBlock {
        testBlockRepository.save(testBlockMapper.map(TestBlockchainBlock(block), status))
        return block
    }

    protected suspend fun saveLog(collection: String, logRecord: TestLogRecord<*>): TestLogRecord<*> {
        return testLogRepository.save(collection, logRecord).awaitFirst()
    }

    protected fun blockEvent(
        block: TestOriginalBlock,
        reverted: TestOriginalBlock? = null,
        source: Source = Source.BLOCKCHAIN
    ): BlockEvent {
        return BlockEvent(
            source,
            TestBlockchainBlock(block).meta,
            reverted?.let { TestBlockchainBlock(reverted).meta }
        )
    }

    protected suspend fun scanOnce(blockScanner: BlockScanner<*, *, *, *>, blockListener: BlockListener) {
        try {
            blockScanner.scan(blockListener)
        } catch (e: IllegalStateException) {
            // Do nothing, in prod there will be infinite attempts count
        }
    }

    protected suspend fun scanOnce(blockchainScanner: BlockchainScanner<*, *, *, *, *, *>) {
        try {
            blockchainScanner.scan()
        } catch (e: IllegalStateException) {
            // Do nothing, in prod there will be infinite attempts count
        }
    }

}
