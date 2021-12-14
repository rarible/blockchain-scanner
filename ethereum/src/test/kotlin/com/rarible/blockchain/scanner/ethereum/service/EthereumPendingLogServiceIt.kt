package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.ethBlock
import com.rarible.blockchain.scanner.ethereum.test.data.ethLog
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomBlockHash
import com.rarible.blockchain.scanner.ethereum.test.data.randomLogHash
import com.rarible.blockchain.scanner.ethereum.test.data.randomLogRecord
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.core.common.nowMillis
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
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
    fun `drop inactive pending logs`() = runBlocking {
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
        val notChangedList = listOf(
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
        val log = EthereumBlockchainLog(
            ethLog(
                transactionHash = transactionHash,
                topic = randomWord(),
                address = randomAddress(),
                logIndex = 0,
                blockHash = randomWord()
            ), 0
        )
        val fullBlock = FullBlock(
            block = EthereumBlockchainBlock(ethBlock(number = 1, hash = randomWord())),
            logs = listOf(log)
        )
        ethereumPendingLogService.dropInactivePendingLogs(fullBlock, descriptor)
        assertNull(findLog(collection, pendingRecord.id))
        for (notChanged in notChangedList) {
            assertEquals(notChanged, findLog(collection, notChanged.id))
        }
    }

    @Test
    fun `drop expired pending logs`() = runBlocking<Unit> {
        val hourAgo = nowMillis().minus(1, ChronoUnit.HOURS)
        val expired = saveLog(
            collection,
            randomLogRecord(topic, randomBlockHash(), status = Log.Status.PENDING)
                .let { it.withLog(it.log.copy(createdAt = hourAgo, updatedAt = hourAgo)) }
        )
        val notExpired = saveLog(collection, randomLogRecord(topic, randomBlockHash(), status = Log.Status.PENDING))
        val otherStatus = saveLog(collection, randomLogRecord(topic, randomBlockHash(), status = Log.Status.PENDING))
        val otherTopic = saveLog(
            collection, randomLogRecord(
                randomWord(),
                randomBlockHash(),
                status = Log.Status.PENDING
            )
        )
        val notChangedList = listOf(notExpired, otherStatus, otherTopic)
        ethereumPendingLogService.dropExpiredPendingLogs(Duration.ofMinutes(1))
        assertNull(findLog(collection, expired.id))
        for (notChanged in notChangedList) {
            val found = findLog(collection, notChanged.id)
            assertEquals(notChanged, found)
        }
    }
}
