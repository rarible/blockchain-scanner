package com.rarible.blockchain.scanner.ethereum

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomPositiveBigInt
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogData
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumTransactionRecord
import com.rarible.contracts.test.erc20.TestERC20
import com.rarible.contracts.test.erc20.TransferEvent
import com.rarible.core.test.wait.BlockingWait
import io.daonomic.rpc.domain.Word
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import scala.jdk.javaapi.CollectionConverters
import scalether.domain.Address
import scalether.domain.response.TransactionReceipt
import java.math.BigInteger

@IntegrationTest
class EthereumScannerIt : AbstractIntegrationTest() {

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

        BlockingWait.waitAssert {
            // Checking Block is in storage, successfully processed
            val block = findBlock(receipt.blockNumber().toLong())!!
            assertEquals(receipt.blockHash().toString(), block.hash)

            // We expect single LogRecord from our single Subscriber
            val allLogs = findAllLogs(collection)
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

    private fun mintAndVerify(beneficiary: Address, value: BigInteger): TransactionReceipt {
        val result = contract.mint(beneficiary, value).execute().verifySuccess()
        assertEquals(contract.balanceOf(beneficiary).call().block()!!, value)
        return result
    }
}
