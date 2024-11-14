package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlockStatus
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.ReversedEthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.DefaultEthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.randomAddress
import com.rarible.blockchain.scanner.ethereum.test.data.randomBlockHash
import com.rarible.blockchain.scanner.ethereum.test.data.randomLog
import com.rarible.blockchain.scanner.ethereum.test.data.randomLogHash
import com.rarible.blockchain.scanner.ethereum.test.data.randomLogRecord
import com.rarible.blockchain.scanner.ethereum.test.data.randomString
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogData
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.dao.DuplicateKeyException

@IntegrationTest
class EthereumLogServiceIt : AbstractIntegrationTest() {

    private lateinit var descriptor: EthereumDescriptor
    private var topic = randomWord()
    private lateinit var storage: EthereumLogRepository

    @BeforeEach
    fun beforeEach() {
        descriptor = testTransferSubscriber.getDescriptor()
        topic = descriptor.ethTopic
        storage = descriptor.storage
    }

    @Test
    fun `delete existing`() = runBlocking {
        val record = randomLogRecord(topic, randomBlockHash())
        val savedRecord = storage.save(record)

        assertNotNull(storage.findLogEvent(record.id))

        ethereumLogService.delete(descriptor, savedRecord)

        assertNull(storage.findLogEvent(record.id))
    }

    @Test
    fun `delete not existing`() = runBlocking {
        val record = randomLogRecord(topic, randomBlockHash())

        ethereumLogService.delete(descriptor, record)

        assertNull(storage.findLogEvent(record.id))
    }

    @Test
    fun `save - new record`() = runBlocking {
        val newLog = randomLogRecord(topic, randomBlockHash())

        ethereumLogService.save(descriptor, listOf(newLog), newLog.blockHash!!.prefixed())

        val savedVisibleRecord = storage.findLogEvent(newLog.id) as ReversedEthereumLogRecord

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

        storage.save(visibleRecord)
        ethereumLogService.save(descriptor, listOf(updatedVisibleRecord), blockHash.prefixed())
        assertThat(storage.findAll().toList()).hasSize(1)

        val savedVisibleRecord = storage.findLogEvent(visibleRecord.id) as ReversedEthereumLogRecord

        val expectedLog = updatedVisibleRecord.log.copy(updatedAt = savedVisibleRecord.log.updatedAt)

        assertNotNull(savedVisibleRecord)
        assertEquals(updatedVisibleRecord.data, savedVisibleRecord.data)
        assertEquals(expectedLog, savedVisibleRecord.log)
    }

    @Test
    fun `save - replace legacy duplicate`() = runBlocking<Unit> {
        val blockHash = randomBlockHash()
        val transactionHash = randomWord()

        val visibleLog = randomLog(
            transactionHash = transactionHash.toString(),
            topic = randomWord(),
            blockHash = blockHash,
            address = randomAddress(),
            status = EthereumBlockStatus.REVERTED
        ).copy(logIndex = 12, minorLogIndex = 2)
        val visibleRecord = randomLogRecord(visibleLog)

        // Assume new log is CONFIRMED and should replace existing one
        val visibleRecordData = visibleRecord.data as TestEthereumLogData
        val updatedVisibleRecord = visibleRecord.copy(
            data = visibleRecordData.copy(customData = randomString()),
            topic = topic, // Topic is different
            status = EthereumBlockStatus.CONFIRMED
        )

        storage.save(visibleRecord)
        ethereumLogService.save(descriptor, listOf(updatedVisibleRecord), blockHash.prefixed())
        assertThat(descriptor.storage.findAll().toList()).hasSize(1)

        val savedVisibleRecord = storage.findLogEvent(visibleRecord.id) as ReversedEthereumLogRecord

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
        storage.save(visibleRecord)
        val errorLog = randomLog(
            transactionHash = existingLog.transactionHash,
            blockHash = existingLog.blockHash!!,
            topic = topic
        ).copy(logIndex = existingLog.logIndex, minorLogIndex = existingLog.minorLogIndex)
        // The following unique index is violated: transactionHash.blockHash.logIndex.minorLogIndex
        assertThrows<DuplicateKeyException> {
            storage.save(randomLogRecord(errorLog))
        }
    }

    @Test
    fun `save - log record not changed`() = runBlocking {
        val log = randomLogRecord(topic, randomBlockHash())

        val savedLog = storage.save(log)
        ethereumLogService.save(descriptor, listOf(log), log.blockHash!!.prefixed())

        val updatedLog = storage.findLogEvent(log.id) as ReversedEthereumLogRecord

        assertNotNull(updatedLog)
        assertEquals(savedLog.version, updatedLog.version)
        assertEquals(log.log, updatedLog.log)
    }

    @Test
    fun `prepare reverted logs`() = runBlocking {
        val anotherStorage = DefaultEthereumLogRepository(mongo, "another")
        val blockHash = randomBlockHash()

        val reverted = storage.save(randomLogRecord(topic, blockHash))
        // wrongBlockHash
        storage.save(randomLogRecord(topic, randomBlockHash()))
        // wrongTopic
        storage.save(randomLogRecord(randomWord(), blockHash))
        // wrongCollection
        anotherStorage.save(randomLogRecord(topic, blockHash))

        val revertedLogs =
            ethereumLogService.prepareLogsToRevertOnRevertedBlock(descriptor, blockHash.toString()).toList()
        assertEquals(1, revertedLogs.size)
        assertEquals(reverted.id, revertedLogs[0].id)
    }

    @RepeatedTest(10)
    fun `revert already reverted transaction`() = runBlocking<Unit> {
        val blockHash = randomBlockHash()
        val transactionHash = randomLogHash()

        val alreadyRevertedRecord = storage.save(
            randomLogRecord(
                topic = topic,
                blockHash = randomBlockHash(),
                transactionHash = transactionHash,
                status = EthereumBlockStatus.REVERTED,
            )
        ) as ReversedEthereumLogRecord
        val confirmedRecord = storage.save(
            alreadyRevertedRecord.copy(
                id = randomString(),
                version = null,
                blockHash = blockHash,
                transactionHash = transactionHash,
                status = EthereumBlockStatus.CONFIRMED,
            )
        ) as ReversedEthereumLogRecord


        val revertedLogs =
            ethereumLogService.prepareLogsToRevertOnRevertedBlock(descriptor, blockHash.toString()).toList()

        ethereumLogService.save(descriptor, revertedLogs, blockHash.toString())

        val records = descriptor.storage.findAll().toList()

        assertThat(records).hasSize(2)

        val revertedConfirmedRecord = storage.findLogEvent(confirmedRecord.id) as ReversedEthereumLogRecord
        assertThat(revertedConfirmedRecord.status).isEqualTo(EthereumBlockStatus.REVERTED)

        val alreadyRevertedRecordNotUpdated = storage.findLogEvent(alreadyRevertedRecord.id)
        assertThat(alreadyRevertedRecordNotUpdated).isEqualTo(alreadyRevertedRecord)
    }
}
