package com.rarible.blockchain.scanner.ethereum.test

import com.rarible.blockchain.scanner.ethereum.EthereumScanner
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.mapper.EthereumBlockMapper
import com.rarible.blockchain.scanner.ethereum.mapper.EthereumLogMapper
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumBlockRepository
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.service.EthereumBlockService
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.service.EthereumPendingLogService
import com.rarible.blockchain.scanner.ethereum.test.subscriber.TestBidSubscriber
import com.rarible.blockchain.scanner.ethereum.test.subscriber.TestTransferSubscriber
import com.rarible.core.task.TaskService
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.runBlocking
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.findAll
import scalether.core.MonoEthereum
import scalether.transaction.MonoTransactionPoller
import scalether.transaction.MonoTransactionSender

@ExperimentalCoroutinesApi
@FlowPreview
abstract class AbstractIntegrationTest {

    @Autowired
    protected lateinit var sender: MonoTransactionSender

    @Autowired
    protected lateinit var poller: MonoTransactionPoller

    @Autowired
    protected lateinit var ethereum: MonoEthereum

    @Autowired
    protected lateinit var mongo: ReactiveMongoOperations

    @Autowired
    protected lateinit var ethereumScanner: EthereumScanner

    @Autowired
    lateinit var ethereumBlockMapper: EthereumBlockMapper

    @Autowired
    lateinit var ethereumBlockRepository: EthereumBlockRepository

    @Autowired
    lateinit var ethereumBlockService: EthereumBlockService

    @Autowired
    lateinit var ethereumLogMapper: EthereumLogMapper

    @Autowired
    lateinit var ethereumLogRepository: EthereumLogRepository

    @Autowired
    lateinit var ethereumLogService: EthereumLogService

    @Autowired
    lateinit var ethereumPendingLogService: EthereumPendingLogService

    @Autowired
    lateinit var testTransferSubscriber: TestTransferSubscriber

    @Autowired
    lateinit var testBidSubscriber: TestBidSubscriber

    @Autowired
    lateinit var taskService: TaskService

    @Autowired
    lateinit var properties: EthereumScannerProperties

    protected fun findLog(collection: String, id: String): EthereumLogRecord<*>? = runBlocking {
        ethereumLogRepository.findLogEvent(ReversedEthereumLogRecord::class.java, collection, id)
    }

    protected fun findBlock(number: Long): EthereumBlock? {
        return mono { ethereumBlockRepository.findById(number) }.block()
    }

    protected fun findAllLogs(collection: String): List<Any> {
        return mongo.findAll<Any>(collection).collectList().block() ?: emptyList()
    }

    protected fun saveLog(collection: String, logRecord: EthereumLogRecord<*>): EthereumLogRecord<*> {
        return mono { ethereumLogRepository.save(collection, logRecord) }.block()!!
    }

    protected fun saveBlock(
        block: EthereumBlock
    ): EthereumBlock {
        return mono { ethereumBlockRepository.save(block) }.block()!!
    }

}
