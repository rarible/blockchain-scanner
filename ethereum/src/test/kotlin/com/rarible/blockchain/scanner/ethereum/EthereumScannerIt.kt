package com.rarible.blockchain.scanner.ethereum

import com.fasterxml.jackson.databind.ObjectMapper
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.task.EthereumBlockReindexTaskHandler
import com.rarible.blockchain.scanner.ethereum.task.EthereumReindexParam
import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.ethBlock
import com.rarible.blockchain.scanner.ethereum.test.data.ethLog
import com.rarible.blockchain.scanner.ethereum.test.data.ethTransaction
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomBlockHash
import com.rarible.blockchain.scanner.ethereum.test.data.randomPositiveBigInt
import com.rarible.blockchain.scanner.ethereum.test.data.randomString
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogData
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumTransactionRecord
import com.rarible.blockchain.scanner.ethereum.test.subscriber.TestSimpleLogSubscriber
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.handler.TypedBlockRange
import com.rarible.blockchain.scanner.reindex.BlockRange
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.contracts.test.erc20.TestERC20
import com.rarible.contracts.test.erc20.TransferEvent
import com.rarible.core.common.nowMillis
import com.rarible.core.test.wait.Wait
import io.daonomic.rpc.domain.Word
import io.mockk.coEvery
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import scala.jdk.javaapi.CollectionConverters
import scalether.domain.Address
import scalether.domain.response.TransactionReceipt
import java.math.BigInteger
import java.time.Duration

@IntegrationTest
class EthereumScannerIt : AbstractIntegrationTest() {

    private lateinit var descriptor: EthereumDescriptor
    private lateinit var topic: Word
    private lateinit var contract: TestERC20

    @Autowired
    private lateinit var manager: EthereumScannerManager

    @Autowired
    private lateinit var blockService: BlockService

    @Autowired
    private lateinit var reindexTaskHandler: EthereumBlockReindexTaskHandler

    @Autowired
    private lateinit var jacksonObjectMapper: ObjectMapper

    @BeforeEach
    fun beforeEach() {
        descriptor = testTransferSubscriber.getDescriptor()
        topic = descriptor.ethTopic

        contract = TestERC20.deployAndWait(sender, poller, "NAME", "NM").block()!!
    }

    @Test
    fun `scan - new block event handled`() = runBlocking<Unit> {

        // Making random mint
        val beneficiary = randomAddress()
        val value = randomPositiveBigInt(1000000)
        val receipt = mintAndVerify(beneficiary, value)

        Wait.waitAssert(timeout = Duration.ofSeconds(30)) {
            // Checking Block is in storage, successfully processed
            val block = findBlock(receipt.blockNumber().toLong())
            assertThat(receipt.blockHash().toString()).isEqualTo(block?.hash)

            // We expect single LogRecord from our single Subscriber
            val allLogs = findAllLogs(descriptor)
            assertThat(allLogs).hasSize(1)

            val testRecord = allLogs.single() as ReversedEthereumLogRecord
            assertThat(testRecord.log.status).isEqualTo(EthereumBlockStatus.CONFIRMED)
            assertThat(testRecord.log.from).isEqualTo(sender.from())
            assertThat(testRecord.log.blockTimestamp).isEqualTo(receipt.getTimestamp().epochSecond)

            val data = testRecord.data as TestEthereumLogData
            assertThat(data).isEqualTo(
                TestEthereumLogData(
                    customData = data.customData,
                    from = Address.ZERO(),
                    to = beneficiary,
                    value = value,
                    transactionInput = receipt.getTransaction().input().toString()
                )
            )
        }

        verifyPublishedLogEvent { logRecordEvent ->
            assertThat(logRecordEvent.reverted).isFalse
            assertThat(logRecordEvent.record).isInstanceOfSatisfying(ReversedEthereumLogRecord::class.java) { record ->
                assertThat(record).isEqualTo(
                    ReversedEthereumLogRecord(
                        id = record.id,
                        version = record.version,
                        transactionHash = receipt.transactionHash().toString(),
                        status = EthereumBlockStatus.CONFIRMED,
                        topic = TransferEvent.id(),
                        minorLogIndex = 0,
                        index = 0,
                        from = sender.from(),
                        to = contract.address(),
                        address = contract.address(),
                        blockHash = receipt.blockHash(),
                        blockNumber = receipt.blockNumber().toLong(),
                        logIndex = CollectionConverters.asJava(receipt.logs()).first().logIndex().toInt(),
                        blockTimestamp = receipt.getTimestamp().epochSecond,
                        visible = true,
                        createdAt = record.createdAt,
                        updatedAt = record.updatedAt,
                        data = record.data
                    )
                )
            }
        }

        verifyPublishedTransactionEvent { transactionRecordEvent ->
            assertThat(transactionRecordEvent.reverted).isFalse()
            assertThat(transactionRecordEvent.record).isEqualTo(
                TestEthereumTransactionRecord(
                    hash = receipt.transactionHash().toString(),
                    input = receipt.getTransaction().input().toString(),
                )
            )
        }
    }

    @Test
    fun `reindex - avoid id duplication in published logEvents`() = runBlocking<Unit> {

        // Making random mint
        val beneficiary = randomAddress()
        val value = randomPositiveBigInt(1000000)
        val receipt = mintAndVerify(beneficiary, value)

        Wait.waitAssert(timeout = Duration.ofSeconds(30)) {
            // Checking Block is in storage, successfully processed
            val block = findBlock(receipt.blockNumber().toLong())
            assertThat(receipt.blockHash().toString()).isEqualTo(block?.hash)

            // We expect single LogRecord from our single Subscriber
            val allLogs = findAllLogs(descriptor)
            assertThat(allLogs).hasSize(1)
        }

        Wait.waitAssert(timeout = Duration.ofSeconds(30)) {
            assertThat(testEthereumLogEventPublisher.publishedLogRecords).hasSize(1)
        }

        // Lets reindex current block
        val baseBlock = blockService.getBlock(receipt.blockNumber().toLong() - 1)!!
        val blockRanges = BlockRanges.getRanges(
            from = receipt.blockNumber().toLong(),
            to = receipt.blockNumber().toLong(),
            step = 1
        ).map { TypedBlockRange(it, true) }.asFlow()
        manager.blockReindexer.reindex(
            baseBlock = baseBlock,
            blocksRanges = blockRanges,
            publisher = manager.logRecordEventPublisher
        ).collect { }

        Wait.waitAssert(timeout = Duration.ofSeconds(30)) {
            val events =
                testEthereumLogEventPublisher.publishedLogRecords.map { it.record as ReversedEthereumLogRecord }
            assertThat(events).hasSize(2)

            // We still have a single LogRecord in db
            val allLogs = findAllLogs(descriptor)
            assertThat(allLogs).hasSize(1)

            // We use random id in the TestTransferSubscriber, we must be sure that id from logEvents are the same
            assertThat(events.first().id).isEqualTo(events.last().id)
        }
    }

    @Test
    fun `reindex - use reconciliation client`() = runBlocking<Unit> {
        val reconciliationClient = testEthereumClientFactory.createReconciliationClient()

        val simpleTopic = TestSimpleLogSubscriber.topic
        val descriptor = TestSimpleLogSubscriber.getDescriptor()

        val blockHash = randomBlockHash()
        val blockNumber = 10
        val logIndex = 1
        val ethBlock = ethBlock(
            number = blockNumber, hash = blockHash, logs = listOf(
                EthereumBlockchainLog(
                    ethLog = ethLog(
                        transactionHash = randomWord(),
                        topic = simpleTopic,
                        address = randomAddress(),
                        logIndex = logIndex,
                        blockHash = blockHash,
                        blockNumber = blockNumber.toBigInteger(),
                    ),
                    ethTransaction = ethTransaction(
                        transactionHash = randomWord(),
                        blockHash = blockHash,
                        blockNumber = blockNumber.toBigInteger(),
                    ),
                    index = logIndex,
                    total = 1
                )
            )
        )
        val block = EthereumBlockchainBlock(ethBlock)

        val parentBlock = EthereumBlockchainBlock(
            number = blockNumber - 1L,
            hash = block.parentHash.toString(),
            parentHash = randomString(),
            timestamp = block.timestamp - 1000L,
            receivedTime = nowMillis(),
            ethBlock = ethBlock(number = blockNumber - 1, block.ethBlock.parentHash())
        )
        val log = EthereumBlockchainLog(
            ethLog = ethLog(
                transactionHash = ethBlock.transactions().head().hash(),
                topic = simpleTopic,
                address = randomAddress(),
                logIndex = logIndex,
                blockHash = blockHash
            ),
            ethTransaction = ethTransaction(
                transactionHash = ethBlock.transactions().head().hash(),
                blockHash = blockHash,
                blockNumber = blockNumber.toBigInteger()
            ),
            index = 0,
            total = 1,
        )

        coEvery {
            reconciliationClient.getBlock(blockNumber.toLong())
        } returns EthereumBlockchainBlock(ethBlock)
        coEvery {
            reconciliationClient.getBlock(blockNumber - 1L)
        } returns parentBlock
        coEvery {
            reconciliationClient.getBlocks(listOf(blockNumber.toLong()))
        } returns listOf(block)
        coEvery {
            reconciliationClient.getLastBlockNumber()
        } returns blockNumber + 100L
        coEvery {
            reconciliationClient.getBlockLogs(match { it.ethTopic == descriptor.ethTopic }, listOf(block), true)
        } returns flowOf(FullBlock(block, listOf(log)))
        coEvery {
            reconciliationClient.getBlockLogs(match { it.ethTopic != descriptor.ethTopic }, listOf(block), true)
        } returns flowOf(FullBlock(block, emptyList()))

        val param = jacksonObjectMapper.writeValueAsString(
            EthereumReindexParam(
                range = BlockRange(blockNumber.toLong(), blockNumber.toLong(), 1),
                useReconciliationRpcNode = true,
                addresses = emptyList(),
                topics = listOf(simpleTopic),
                publishEvents = true,
            )
        )
        val reindexedBlock = reindexTaskHandler.runLongTask(null, param).toList()
        assertThat(reindexedBlock).containsExactly(blockNumber.toLong())

        Wait.waitAssert(timeout = Duration.ofSeconds(30)) {
            val events =
                testEthereumLogEventPublisher.publishedLogRecords.map { it.record as ReversedEthereumLogRecord }
            assertThat(events).hasSize(1)
        }
    }

    private fun mintAndVerify(beneficiary: Address, value: BigInteger): TransactionReceipt {
        val result = contract.mint(beneficiary, value).execute().verifySuccess()
        assertEquals(contract.balanceOf(beneficiary).call().block()!!, value)
        return result
    }
}
