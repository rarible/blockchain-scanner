package com.rarible.blockchain.scanner.ethereum

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.*
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogRecord
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.reconciliation.ReconciliationTaskHandler
import com.rarible.contracts.test.erc20.TestERC20
import com.rarible.contracts.test.erc20.TransferEvent
import com.rarible.core.common.nowMillis
import com.rarible.core.task.Task
import com.rarible.core.task.TaskStatus
import com.rarible.core.test.ext.EthereumTest
import com.rarible.core.test.wait.BlockingWait
import io.daonomic.rpc.domain.Word
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.types.ObjectId
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

@EthereumTest

@IntegrationTest
class EthereumScannerIt : AbstractIntegrationTest() {

    private val logger = LoggerFactory.getLogger(EthereumScannerIt::class.java)

    private var descriptor: EthereumDescriptor = mockk()
    private var collection = ""
    private var topic = randomWord()
    private var contract: TestERC20 = mockk()


    @BeforeEach
    fun beforeEach() {
        descriptor = testTransferSubscriber.getDescriptor()
        collection = descriptor.collection
        topic = descriptor.topic

        clearMocks(testLogEventListener)
        coEvery { testLogEventListener.onBlockLogsProcessed(any()) } returns Unit

        contract = TestERC20.deployAndWait(sender, poller, "NAME", "NM").block()!!
    }

    @Test
    fun `scan - new block event handled`() = BlockingWait.waitAssert {

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
            assertEquals(Block.Status.SUCCESS, block.status)

            // We expect single LogRecord from our single Subscriber
            val testRecord = findAllLogs(collection)[0] as TestEthereumLogRecord
            assertEquals(testRecord.log!!.status, Log.Status.CONFIRMED)
            assertEquals(testRecord.from, Address.ZERO())
            assertEquals(testRecord.to, beneficiary)
            assertEquals(testRecord.value, value)
        }

        coVerify(exactly = 1) {
            testLogEventListener.onBlockLogsProcessed(match {
                assertEquals(receipt.blockHash().toString(), it.event.block.hash)
                assertEquals(1, it.records.size)
                true
            })
        }
    }

    @Test
    fun `scan - mark pending logs`() {
        // First expected LogRecord - from mint
        val value = randomPositiveBigInt(1000000)
        mintAndVerify(sender.from(), value)
        assertCollectionSize(collection, 1)

        // Second - from transfer
        val beneficiary = randomAddress()
        val transferReceipt = contract.transfer(beneficiary, value)
            .execute()
            .verifySuccess()

        // Artificial PENDING log for transaction already exist
        val tx = ethereum.ethGetTransactionByHash(transferReceipt.transactionHash()).block()!!.get()
        val log = ethLog(tx.hash().toString(), tx.nonce().toLong())
        val record = ethRecord(log, beneficiary, value)

        val saved = saveLog(collection, record)

        BlockingWait.waitAssert {
            // So in total we have 3 records in storage
            assertEquals(3, findAllLogs(collection).size)

            // First two records should be in CONFIRMED state, they are correct
            val confirmed = findAllLogs(collection).map {
                it as TestEthereumLogRecord
            }.filter {
                it.id != saved.id
            }
            confirmed.forEach {
                val confirmedLog = it.log!!
                assertEquals(confirmedLog.status, Log.Status.CONFIRMED)
                assertNotNull(confirmedLog.blockHash)
                assertNotNull(confirmedLog.blockNumber)
                assertNotNull(confirmedLog.logIndex)
            }

            // Artificial pending record should become INACTIVE
            val inactive = findLog(collection, saved.id)!!
            assertEquals(Log.Status.INACTIVE, inactive.log!!.status)

            val block = findBlock(tx.blockNumber().toLong())
            assertEquals(Block.Status.SUCCESS, block!!.status)
        }
    }

    @Test
    fun `scan - revert pending logs`() {
        // First expected LogRecord - from mint
        val value = randomPositiveBigInt(1000000)
        mintAndVerify(sender.from(), value)
        assertCollectionSize(collection, 1)

        // Second record should not be stored, transfer completed with error
        val beneficiary = randomAddress()
        val transferReceipt = contract
            .transfer(beneficiary, value)
            .withGas(BigInteger.valueOf(23000))
            .execute().verifyError()

        // Artificial PENDING LogRecord for failed transfer
        val log = ethLog(transferReceipt.transactionHash().toString(), 0)
        val record = ethRecord(log, beneficiary, value)
        val saved = saveLog(collection, record)

        BlockingWait.waitAssert {
            // We expect 2 records - first from mint and artificial one with original status PENDING
            assertCollectionSize(collection, 2)

            // PENDING LogRecord should becoe INACTIVE since transfer failed
            val savedLog = findLog(collection, saved.id)!!.log!!
            assertEquals(savedLog.status, Log.Status.INACTIVE)
            assertNull(savedLog.blockNumber)
            assertNull(savedLog.logIndex)
        }
    }

    @Test
    fun `scan - drop cancelled`() {
        // First expected LogRecord - from mint
        val value = randomPositiveBigInt(1000000)
        mintAndVerify(sender.from(), value)
        assertCollectionSize(collection, 1)

        // Second artificial record contains non-existing transaction hash
        val beneficiary = randomAddress()
        val nonce = ethereum.ethGetTransactionCount(sender.from(), "latest").block()!!.toLong()
        val fakeHash = randomLogHash()

        val log = ethLog(fakeHash, nonce)
        val record = ethRecord(log, beneficiary, value)
        val saved = saveLog(collection, record)

        // Let's trigger some BlockEvent to process pending logs
        TestERC20.deploy(sender, "NAME", "NM").verifySuccess()

        BlockingWait.waitAssert {
            val readLog = findLog(collection, saved.id)!!.log!!
            assertEquals(Log.Status.DROPPED, readLog.status)
            assertNull(readLog.blockNumber)
            assertNull(readLog.logIndex)
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
            param = TransferEvent.id().toString(),
            lastStatus = TaskStatus.NONE,
            state = number + 1,
            running = false
        )
        logger.info("Saving task for reconciliation: [{}]", newTask)
        mongo.save(newTask).block()

        taskService.readAndRun()

        // Waiting job is completed and our collection have same count of LogRecords as it had before cleanup
        BlockingWait.waitAssert {
            val tasks = runBlocking { taskService.findTasks(ReconciliationTaskHandler.RECONCILIATION).toList() }
            assertEquals(1, tasks.size)
            assertEquals(TaskStatus.COMPLETED, tasks[0].lastStatus)
            assertEquals(beforeCleanupSize, findAllLogs(collection).size)
        }
    }

    @Test
    fun `pending blocks job`() {
        // Making random mint
        val beneficiary = randomAddress()
        val value = randomPositiveBigInt(1000000)
        val receipt = mintAndVerify(beneficiary, value)
        assertCollectionSize(collection, 1)

        BlockingWait.waitAssert {
            // Checking Block is in storage, successfully processed
            val block = findBlock(receipt.blockNumber().toLong())!!
            assertEquals(Block.Status.SUCCESS, block.status)
        }

        mongo.findAllAndRemove<Any>(Query(), collection).collectList().block()!!

        // Saving same block with status PENDING and timestamp old enough
        val block = findBlock(receipt.blockNumber().toLong())!!
        val pendingBlock = block.copy(timestamp = nowMillis().minusSeconds(61).epochSecond)
        saveBlock(pendingBlock, Block.Status.PENDING)

        BlockingWait.waitAssert {
            // Checking Block is in storage, successfully processed
            val reProcessedBlock = findBlock(receipt.blockNumber().toLong())!!
            assertEquals(receipt.blockHash().toString(), reProcessedBlock.hash)
            assertEquals(Block.Status.SUCCESS, reProcessedBlock.status)

            // Checking records restored successfully
            val testRecord = findAllLogs(collection)[0] as TestEthereumLogRecord
            assertEquals(testRecord.log!!.status, Log.Status.CONFIRMED)
            assertEquals(testRecord.from, Address.ZERO())
            assertEquals(testRecord.to, beneficiary)
            assertEquals(testRecord.value, value)
        }

        // We should get here 2 events - first from initial BlockEvent, second - from indexing pending Block
        coVerify(exactly = 2) {
            testLogEventListener.onBlockLogsProcessed(match {
                assertEquals(receipt.blockHash().toString(), it.event.block.hash)
                assertEquals(1, it.records.size)
                true
            })
        }
    }

    private fun ethRecord(log: EthereumLog, beneficiary: Address, value: BigInteger): TestEthereumLogRecord {
        return TestEthereumLogRecord(
            id = ObjectId(),
            version = null,
            log = log,
            customData = randomString(),
            from = sender.from(),
            to = beneficiary,
            value = value
        )
    }

    private fun ethLog(transactionHash: String, nonce: Long): EthereumLog {
        return EthereumLog(
            address = contract.address(),
            topic = TransferEvent.id(),
            transactionHash = transactionHash,
            from = sender.from(),
            nonce = nonce,
            status = Log.Status.PENDING,
            index = 0,
            minorLogIndex = 0,
            visible = true
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
            assertEquals(expectedSize, findAllLogs(collection).size)
        }
    }

}