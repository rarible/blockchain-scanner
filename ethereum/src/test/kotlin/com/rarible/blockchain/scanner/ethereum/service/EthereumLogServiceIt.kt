package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomBlockHash
import com.rarible.blockchain.scanner.ethereum.test.data.randomLog
import com.rarible.blockchain.scanner.ethereum.test.data.randomLogRecord
import com.rarible.blockchain.scanner.ethereum.test.data.randomString
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogData
import io.mockk.mockk
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.query.Query

@IntegrationTest
class EthereumLogServiceIt : AbstractIntegrationTest() {

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
    fun `delete existing`() = runBlocking {
        val record = randomLogRecord(descriptor.ethTopic, randomBlockHash())
        val savedRecord = saveLog(collection, record)

        assertNotNull(findLog(collection, record.id))

        ethereumLogService.delete(descriptor, savedRecord)

        assertNull(findLog(collection, record.id))
    }

    @Test
    fun `delete not existing`() = runBlocking {
        val record = randomLogRecord(descriptor.ethTopic, randomBlockHash())

        ethereumLogService.delete(descriptor, record)

        assertNull(findLog(collection, record.id))
    }

    @Test
    fun `save - new record`() = runBlocking {
        val newLog = randomLogRecord(topic, randomBlockHash())

        ethereumLogService.save(descriptor, listOf(newLog))

        val savedVisibleRecord = findLog(collection, newLog.id) as ReversedEthereumLogRecord

        assertNotNull(savedVisibleRecord)
        assertEquals(savedVisibleRecord.data, newLog.data)
        assertEquals(newLog.log, savedVisibleRecord.log)
    }

    @Test
    fun `save - overwrite existing`() = runBlocking {
        val blockHash = randomBlockHash()
        val transactionHash = randomWord()

        val visibleLog = randomLog(
            transactionHash = transactionHash.toString(),
            topic = topic,
            blockHash = blockHash,
            address = randomAddress()
        ).copy(index = 2, minorLogIndex = 3)
        val visibleRecord = randomLogRecord(visibleLog)

        // Let's change custom data in order to detect changes
        val visibleRecordData = visibleRecord.data as TestEthereumLogData
        val updatedVisibleRecord = visibleRecord.copy(data = visibleRecordData.copy(customData = randomString()))

        saveLog(descriptor.collection, visibleRecord)
        ethereumLogService.save(descriptor, listOf(updatedVisibleRecord))
        assertEquals(1, mongo.count(Query(), descriptor.collection).awaitFirst())

        val savedVisibleRecord = findLog(collection, visibleRecord.id) as ReversedEthereumLogRecord

        val expectedLog = updatedVisibleRecord.log.copy(updatedAt = savedVisibleRecord.log.updatedAt)

        assertNotNull(savedVisibleRecord)
        assertEquals(updatedVisibleRecord.data, savedVisibleRecord.data)
        assertEquals(expectedLog, savedVisibleRecord.log)
    }

    @Test
    fun `save - throw inconsistency error on saving identical log event`() = runBlocking<Unit> {
        val existingLog = randomLog(
            transactionHash = randomWord().toString(),
            topic = topic,
            blockHash = randomWord(),
            address = randomAddress()
        )
        val visibleRecord = randomLogRecord(existingLog)
        saveLog(descriptor.collection, visibleRecord)
        val errorLog = randomLog(
            transactionHash = existingLog.transactionHash,
            blockHash = existingLog.blockHash!!,
            topic = topic
        ).copy(logIndex = existingLog.logIndex, minorLogIndex = existingLog.minorLogIndex)
        // The following unique index is violated: transactionHash.blockHash.logIndex.minorLogIndex
        assertThrows<DuplicateKeyException> {
            saveLog(descriptor.collection, randomLogRecord(errorLog))
        }
    }

    @Test
    fun `save - log record not changed`() = runBlocking {
        val log = randomLogRecord(topic, randomBlockHash())

        val savedLog = saveLog(collection, log)
        ethereumLogService.save(descriptor, listOf(log))

        val updatedLog = findLog(collection, log.id) as ReversedEthereumLogRecord

        assertNotNull(updatedLog)
        assertEquals(savedLog.version, updatedLog.version)
        assertEquals(log.log, updatedLog.log)
    }

    @Test
    fun `prepare reverted logs`() = runBlocking {
        val anotherCollection = testBidSubscriber.getDescriptor().collection
        val blockHash = randomBlockHash()

        val reverted = saveLog(collection, randomLogRecord(topic, blockHash))
        // wrongBlockHash
        saveLog(collection, randomLogRecord(topic, randomBlockHash()))
        // wrongTopic
        saveLog(collection, randomLogRecord(randomWord(), blockHash))
        // wrongCollection
        saveLog(anotherCollection, randomLogRecord(topic, blockHash))

        val revertedLogs = ethereumLogService.prepareLogsToRevertOnRevertedBlock(descriptor, blockHash.toString()).toList()
        assertEquals(1, revertedLogs.size)
        assertEquals(reverted.id, revertedLogs[0].id)
    }
}
