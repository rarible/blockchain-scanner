package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.ethBlock
import com.rarible.blockchain.scanner.ethereum.test.data.ethLog
import com.rarible.blockchain.scanner.ethereum.test.data.ethTransaction
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomBlockHash
import com.rarible.blockchain.scanner.ethereum.test.data.randomLogHash
import com.rarible.blockchain.scanner.ethereum.test.data.randomLogRecord
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.core.common.nowMillis
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.temporal.ChronoUnit

@FlowPreview
@ExperimentalCoroutinesApi
@IntegrationTest
class EthereumPendingLogServiceIt : AbstractIntegrationTest() {

    private var descriptor: EthereumDescriptor = mockk()
    private var collection = ""
    private var topic = randomWord()

    @BeforeEach
    fun beforeEach() {
        descriptor = testTransferSubscriber.getDescriptor()
        collection = descriptor.collection
        topic = descriptor.ethTopic
    }

    @Test
    fun `get inactive pending logs`() = runBlocking<Unit> {
        val transactionHash = randomWord()
        val pendingRecord = saveLog(
            collection,
            randomLogRecord(
                topic = topic,
                blockHash = null,
                transactionHash = transactionHash.toString(),
                status = Log.Status.PENDING
            )
        )
        listOf(
            // another status
            saveLog(
                collection, randomLogRecord(
                    topic = topic,
                    blockHash = null,
                    transactionHash = transactionHash.toString(),
                    status = Log.Status.CONFIRMED
                )
            ),
            // another transaction hash
            saveLog(
                collection, randomLogRecord(
                    topic = topic,
                    blockHash = null,
                    transactionHash = randomLogHash(),
                    status = Log.Status.PENDING
                )
            ),
        )
        val blockchainLog = EthereumBlockchainLog(
            ethLog = ethLog(
                transactionHash = transactionHash,
                topic = randomWord(),
                address = randomAddress(),
                logIndex = 0,
                blockHash = randomWord()
            ),
            ethTransaction = ethTransaction(),
            index = 0
        )
        val fullBlock = FullBlock(
            block = EthereumBlockchainBlock(ethBlock(number = 1, hash = randomWord())),
            logs = listOf(blockchainLog)
        )
        val pendingLogs = ethereumPendingLogService.getInactivePendingLogs(fullBlock, descriptor)
        assertThat(pendingLogs).isEqualTo(listOf(pendingRecord.withLog(pendingRecord.log.copy(status = Log.Status.INACTIVE))))
    }

    @Test
    fun `find expired pending logs`() = runBlocking<Unit> {
        val manyHoursAgo = nowMillis().minus(24, ChronoUnit.HOURS)
        val expired = saveLog(
            collection,
            randomLogRecord(topic, randomBlockHash(), status = Log.Status.PENDING)
                .let { it.withLog(it.log.copy(createdAt = manyHoursAgo, updatedAt = manyHoursAgo)) }
        )
        saveLog(collection, randomLogRecord(topic, randomBlockHash(), status = Log.Status.PENDING))
        saveLog(
            collection, randomLogRecord(
                randomWord(),
                randomBlockHash(),
                status = Log.Status.PENDING
            )
        )
        val fullBlock = mockk<FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>> {
            every { logs } returns emptyList()
        }
        val expiredLogs = ethereumPendingLogService.getInactivePendingLogs(fullBlock, descriptor)
        assertThat(expiredLogs).isEqualTo(listOf(expired.withLog(expired.log.copy(status = Log.Status.DROPPED))))
    }
}
