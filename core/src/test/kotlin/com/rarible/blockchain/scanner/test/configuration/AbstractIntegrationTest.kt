package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.blockchain.scanner.event.block.*
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.client.TestOriginalBlock
import com.rarible.blockchain.scanner.test.model.TestCustomLogRecord
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.repository.TestLogRepository
import com.rarible.blockchain.scanner.test.service.TestLogService
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.findAll

abstract class AbstractIntegrationTest {

    @Autowired
    protected lateinit var mongo: ReactiveMongoOperations

    @Autowired
    lateinit var testBlockRepository: BlockRepository

    @Autowired
    lateinit var testBlockService: BlockService

    @Autowired
    lateinit var testLogRepository: TestLogRepository

    @Autowired
    lateinit var testLogService: TestLogService

    @Autowired
    lateinit var properties: TestBlockchainScannerProperties

    protected suspend fun findLog(collection: String, id: Long): TestLogRecord? {
        return testLogRepository.findLogEvent(TestCustomLogRecord::class.java, collection, id)
    }

    protected suspend fun findBlock(number: Long): Block? {
        return testBlockRepository.findById(number)
    }

    protected suspend fun findAllLogs(collection: String): List<Any> {
        return mongo.findAll<Any>(collection).collectList().awaitFirst()
    }

    protected suspend fun findAllBlocks(): List<Block> {
        return mongo.findAll<Block>().collectList().awaitFirst()
    }

    protected suspend fun saveBlock(
        block: TestOriginalBlock
    ): TestOriginalBlock {
        testBlockRepository.save(mapBlockchainBlock(TestBlockchainBlock(block)))
        return block
    }


    protected fun newBlockEvent(
        block: TestOriginalBlock,
        source: Source = Source.BLOCKCHAIN
    ): NewBlockEvent {
        return NewBlockEvent(source, block.number, block.hash)
    }

    protected fun revertedBlockEvent(
        block: TestOriginalBlock,
        source: Source = Source.BLOCKCHAIN
    ): RevertedBlockEvent {
        return RevertedBlockEvent(source, block.number, block.hash)
    }

    protected suspend fun BlockScanner<*>.scanOnce(publisher: BlockEventPublisher) {
        try {
            scan(publisher)
        } catch (e: Exception) {
            // Ignore the flow completed.
        }
    }

    protected suspend fun BlockchainScanner<*, *, *, *>.scanOnce() {
        try {
            scan()
        } catch (e: IllegalStateException) {
            // Do nothing, in prod there will be infinite attempts count
        }
    }
}
