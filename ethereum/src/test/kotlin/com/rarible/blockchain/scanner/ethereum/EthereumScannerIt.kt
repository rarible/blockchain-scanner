package com.rarible.blockchain.scanner.ethereum

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
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogStatus
import com.rarible.contracts.test.erc20.TestERC20
import com.rarible.contracts.test.erc20.TransferEvent
import com.rarible.core.common.nowMillis
import com.rarible.core.test.wait.BlockingWait
import io.daonomic.rpc.domain.Word
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
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
            assertThat(testRecord.log.status).isEqualTo(EthereumLogStatus.CONFIRMED)
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
            assertThat(logRecordEvent.record).isInstanceOfSatisfying(ReversedEthereumLogRecord::class.java) {
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

        verifyPublishedLogEvent { logRecord ->
            assertThat(logRecord.reverted).isTrue()
            assertThat(logRecord.record).isInstanceOfSatisfying(ReversedEthereumLogRecord::class.java) {
                assertThat(it.id).isEqualTo(pendingLog.id)
                assertThat(it.log.status).isEqualTo(EthereumLogStatus.INACTIVE)
            }
        }
    }

    private fun ethRecord(log: EthereumLog, beneficiary: Address, value: BigInteger): ReversedEthereumLogRecord {
        return ReversedEthereumLogRecord(
            id = randomString(),
            version = null,
            log = log,
            data = TestEthereumLogData(
                customData = randomString(),
                from = sender.from(),
                to = beneficiary,
                value = value,
                transactionInput = randomString()
            )
        )
    }

    private fun ethLog(transactionHash: String): EthereumLog {
        return EthereumLog(
            address = contract.address(),
            topic = TransferEvent.id(),
            transactionHash = transactionHash,
            status = EthereumLogStatus.PENDING,
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
}
