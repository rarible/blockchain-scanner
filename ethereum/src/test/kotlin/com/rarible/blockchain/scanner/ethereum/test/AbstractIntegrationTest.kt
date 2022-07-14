@file:OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)

package com.rarible.blockchain.scanner.ethereum.test

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.service.EthereumLogService
import com.rarible.blockchain.scanner.ethereum.test.subscriber.TestBidSubscriber
import com.rarible.blockchain.scanner.ethereum.test.subscriber.TestTransferSubscriber
import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.core.task.TaskService
import com.rarible.core.test.wait.BlockingWait
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.data.mongodb.core.findAll
import reactor.core.publisher.Mono
import scalether.core.MonoEthereum
import scalether.domain.response.Transaction
import scalether.domain.response.TransactionReceipt
import scalether.transaction.MonoTransactionPoller
import scalether.transaction.MonoTransactionSender
import java.time.Instant
import kotlin.concurrent.withLock

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
    lateinit var ethereumBlockRepository: BlockRepository

    @Autowired
    lateinit var ethereumBlockService: BlockService

    @Autowired
    lateinit var ethereumLogRepository: EthereumLogRepository

    @Autowired
    lateinit var ethereumLogService: EthereumLogService

    @Autowired
    lateinit var testTransferSubscriber: TestTransferSubscriber

    @Autowired
    lateinit var monoEthereum: MonoEthereum

    @Autowired
    @Qualifier("testEthereumBlockchainClient")
    lateinit var testEthereumBlockchainClient: TestEthereumBlockchainClient

    @Autowired
    @Qualifier("testEthereumLogEventPublisher")
    lateinit var testEthereumLogEventPublisher: TestEthereumLogRecordEventPublisher

    @Autowired
    lateinit var testBidSubscriber: TestBidSubscriber

    @Autowired
    lateinit var taskService: TaskService

    @Autowired
    lateinit var properties: EthereumScannerProperties

    protected fun findLog(collection: String, id: String): EthereumLogRecord? = runBlocking {
        ethereumLogRepository.findLogEvent(ReversedEthereumLogRecord::class.java, collection, id)
    }

    protected fun findBlock(number: Long): Block? {
        return mono { ethereumBlockRepository.findById(number) }.block()
    }

    protected fun findAllLogs(collection: String): List<Any> {
        return mongo.findAll<Any>(collection).collectList().block() ?: emptyList()
    }

    protected fun saveLog(collection: String, logRecord: EthereumLogRecord): EthereumLogRecord {
        return mono { ethereumLogRepository.save(collection, logRecord) }.block()!!
    }

    protected fun saveBlock(
        block: Block
    ): Block {
        return mono { ethereumBlockRepository.save(block) }.block()!!
    }

    protected fun <T> delayBlockHandling(block: () -> T): T {
        return testEthereumBlockchainClient.blocksDelayLock.withLock {
            block()
        }
    }

    @BeforeEach
    fun ignoreOldBlocks() = runBlocking<Unit> {
        val currentBlockNumber = monoEthereum.ethBlockNumber().awaitFirst().toLong()
        testEthereumBlockchainClient.startingBlock = currentBlockNumber + 1
    }

    @BeforeEach
    fun cleanupLogs() {
        testEthereumLogEventPublisher.publishedLogRecords.clear()
    }

    protected fun verifyPublishedLogEvent(asserter: (LogRecordEvent) -> Unit) {
        BlockingWait.waitAssert {
            assertThat(testEthereumLogEventPublisher.publishedLogRecords).anySatisfy(asserter)
        }
    }

    protected fun TransactionReceipt.getTimestamp(): Instant =
        Instant.ofEpochSecond(ethereum.ethGetFullBlockByHash(blockHash()).map { it.timestamp() }.block()!!.toLong())

    protected fun TransactionReceipt.getTransaction(): Transaction =
        ethereum.ethGetTransactionByHash(transactionHash()).block()!!.get()

    protected fun Mono<Word>.verifySuccess(): TransactionReceipt {
        val receipt = waitReceipt()
        Assertions.assertTrue(receipt.success())
        return receipt
    }

    protected fun Mono<Word>.waitReceipt(): TransactionReceipt {
        val value = this.block()
        require(value != null) { "Transaction hash is null" }
        return ethereum.ethGetTransactionReceipt(value).block()!!.get()
    }
}
