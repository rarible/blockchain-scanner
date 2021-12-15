package com.rarible.blockchain.scanner.ethereum

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomPositiveBigInt
import com.rarible.blockchain.scanner.ethereum.test.data.randomPositiveInt
import com.rarible.blockchain.scanner.ethereum.test.data.randomString
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogData
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.reconciliation.ReconciliationTaskHandler
import com.rarible.contracts.test.erc20.TestERC20
import com.rarible.contracts.test.erc20.TransferEvent
import com.rarible.core.common.nowMillis
import com.rarible.core.task.Task
import com.rarible.core.task.TaskStatus
import com.rarible.core.test.wait.BlockingWait
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.lt
import reactor.core.publisher.Mono
import scalether.domain.Address
import scalether.domain.response.TransactionReceipt
import java.math.BigInteger

@FlowPreview
@IntegrationTest
@ExperimentalCoroutinesApi
class EthereumScannerIt : AbstractIntegrationTest() {

    private val logger = LoggerFactory.getLogger(EthereumScannerIt::class.java)

    private lateinit var descriptor: EthereumDescriptor
    private lateinit var collection: String
    private lateinit var topic: Word
    private lateinit var contract: TestERC20

    @BeforeEach
    fun beforeEach() {
        descriptor = testTransferSubscriber.getDescriptor()
        collection = descriptor.collection
        topic = descriptor.ethTopic

        contract = TestERC20.deployAndWait(sender, poller, "NAME", "NM").block()!!
    }

    @Test
    fun `scan - new block event handled`() = BlockingWait.waitAssert<Unit> {

        // Making random mint
        val beneficiary = randomAddress()
        val value = randomPositiveBigInt(1000000)
        val receipt = mintAndVerify(beneficiary, value)

        // Ensure we have something in DB - it means, scanner caught event
        assertCollectionSize(collection, 1)

        BlockingWait.waitAssert {
            // Checking Block is in storage, successfully processed
            val block = findBlock(receipt.blockNumber().toLong())!!
            assertEquals(receipt.blockHash().toString(), block.hash)

            // We expect single LogRecord from our single Subscriber
            val testRecord = findAllLogs(collection)[0] as ReversedEthereumLogRecord
            val data = testRecord.data as TestEthereumLogData
            assertThat(testRecord.log.status).isEqualTo(Log.Status.CONFIRMED)
            assertThat(data.from).isEqualTo(Address.ZERO())
            assertThat(data.to).isEqualTo(beneficiary)
            assertThat(data.value).isEqualTo(value)
        }

        verifyPublishedLogEvent { logEvent ->
            assertThat(logEvent).isInstanceOfSatisfying(ReversedEthereumLogRecord::class.java) {
                assertThat(it.transactionHash).isEqualTo(receipt.transactionHash().toString())
                assertThat(it.data).isInstanceOfSatisfying(TestEthereumLogData::class.java) { logData ->
                    assertThat(logData.to).isEqualTo(beneficiary)
                }
            }
        }
    }

    @Test
    fun `scan - mark pending logs as inactive`() {
        val value = randomPositiveBigInt(1000000)
        val pendingLog = delayBlockHandling {
            // First - log from mint
            mintAndVerify(sender.from(), value)

            // Second - log from transfer
            val beneficiary = randomAddress()
            val transferReceipt = contract.transfer(beneficiary, value)
                .execute()
                .verifySuccess()

            // Artificial PENDING log for transaction that already exists
            val log = ethLog(transferReceipt.transactionHash().toString()).copy(
                index = randomPositiveInt()
            )
            val record = ethRecord(log, beneficiary, value)

            saveLog(collection, record)
        }

        BlockingWait.waitAssert {
            // The inactive log must be removed.
            assertNull(findLog(collection, pendingLog.id))
        }

        verifyDismissedLogEvent { logRecord ->
            assertThat(logRecord).isInstanceOfSatisfying(ReversedEthereumLogRecord::class.java) {
                assertThat(it.id).isEqualTo(pendingLog.id)
                assertThat(it.log.status).isEqualTo(Log.Status.INACTIVE)
            }
        }
    }

    @Test
    fun `reconciliation job`() {
        val number = ethereum.ethBlockNumber().block()!!.toLong()
        val beneficiary = randomAddress()
        val value = randomPositiveBigInt(100000)
        mintAndVerify(beneficiary, value)
        assertCollectionSize(collection, 1)

        val numberEnd = ethereum.ethBlockNumber().block()!!.toLong()
        val beforeCleanupSize = findAllLogs(collection).size

        // Cleanup Blocks and Task collection to trigger reconciliation job
        mongo.findAllAndRemove(Query(), Task::class.java).then().block()
        mongo.findAllAndRemove(Query(EthereumBlock::id lt numberEnd), EthereumBlock::class.java).then().block()
        mongo.findAllAndRemove<Any>(Query(), collection).collectList().block()!!

        val newTask = Task(
            type = ReconciliationTaskHandler.RECONCILIATION,
            param = "transfers",
            lastStatus = TaskStatus.NONE,
            state = number + 1,
            running = false
        )
        logger.info("Saving task for reconciliation: [{}]", newTask)
        mongo.save(newTask).block()

        taskService.readAndRun()

        // Waiting job is completed and our collection have same number of LogRecords as it had before cleanup
        BlockingWait.waitAssert {
            val tasks = runBlocking { taskService.findTasks(ReconciliationTaskHandler.RECONCILIATION).toList() }
            assertEquals(1, tasks.size)
            assertEquals(TaskStatus.COMPLETED, tasks[0].lastStatus)
            assertEquals(beforeCleanupSize, findAllLogs(collection).size)
        }
    }

    private fun ethRecord(log: EthereumLog, beneficiary: Address, value: BigInteger): ReversedEthereumLogRecord {
        return ReversedEthereumLogRecord(
            id = randomString(),
            version = null,
            log = log,
            TestEthereumLogData(
                customData = randomString(),
                from = sender.from(),
                to = beneficiary,
                value = value
            )
        )
    }

    private fun ethLog(transactionHash: String): EthereumLog {
        return EthereumLog(
            address = contract.address(),
            topic = TransferEvent.id(),
            transactionHash = transactionHash,
            status = Log.Status.PENDING,
            index = 0,
            minorLogIndex = 0,
            visible = true,
            createdAt = nowMillis(),
            updatedAt = nowMillis()
        )
    }

    private fun mintAndVerify(beneficiary: Address, value: BigInteger): TransactionReceipt {
        val result = contract.mint(beneficiary, value).execute().verifySuccess()
        assertEquals(contract.balanceOf(beneficiary).call().block()!!, value)
        return result
    }

    private fun Mono<Word>.verifyError(): TransactionReceipt {
        val receipt = waitReceipt()
        assertFalse(receipt.success())
        return receipt
    }

    private fun Mono<Word>.verifySuccess(): TransactionReceipt {
        val receipt = waitReceipt()
        assertTrue(receipt.success())
        return receipt
    }

    private fun Mono<Word>.waitReceipt(): TransactionReceipt {
        val value = this.block()
        require(value != null) { "Transaction hash is null" }
        return ethereum.ethGetTransactionReceipt(value).block()!!.get()
    }

    private fun assertCollectionSize(collection: String, expectedSize: Int) {
        BlockingWait.waitAssert {
            assertThat(findAllLogs(collection)).hasSize(expectedSize)
        }
    }

}
