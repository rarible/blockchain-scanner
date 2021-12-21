package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.randomBlockHash
import com.rarible.blockchain.scanner.ethereum.test.data.randomLogRecord
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.blockchain.scanner.framework.model.Log
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@IntegrationTest
class EthereumLogRepositoryTest : AbstractIntegrationTest() {

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
    fun `find pending logs`() = runBlocking {
        val anotherCollection = testBidSubscriber.getDescriptor().collection

        val pendingLog = saveLog(collection, randomLogRecord(topic, randomBlockHash(), status = Log.Status.PENDING))
        saveLog(collection, randomLogRecord(topic, randomBlockHash(), status = Log.Status.CONFIRMED))
        saveLog(collection, randomLogRecord(topic, randomBlockHash(), status = Log.Status.REVERTED))
        saveLog(anotherCollection, randomLogRecord(topic, randomBlockHash(), status = Log.Status.PENDING))

        val pendingLogs = ethereumLogRepository.findPendingLogs(
            entityType = descriptor.entityType,
            collection = descriptor.collection,
            topic = descriptor.ethTopic
        ).toList()

        Assertions.assertEquals(1, pendingLogs.size)
        Assertions.assertEquals(pendingLog, pendingLogs[0])
    }
}
