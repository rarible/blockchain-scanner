package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.*
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumLogRecord
import com.rarible.blockchain.scanner.framework.model.Log
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@FlowPreview
@ExperimentalCoroutinesApi
@IntegrationTest
class EthereumLogServiceIt : AbstractIntegrationTest() {

    private var descriptor: EthereumDescriptor = mockk()
    private var collection = ""
    private var topic = randomWord()

    @BeforeEach
    fun beforeEach() {
        descriptor = testTransferSubscriber.getDescriptor()
        collection = descriptor.collection
        topic = descriptor.topic
    }

    @Test
    fun `delete existing`() = runBlocking {
        val record = randomLogRecord(descriptor.topic, randomBlockHash())
        val savedRecord = saveLog(collection, record)

        assertNotNull(findLog(collection, record.id))

        ethereumLogService.delete(descriptor, savedRecord)

        assertNull(findLog(collection, record.id))
    }

    @Test
    fun `delete not existing`() = runBlocking {
        val record = randomLogRecord(descriptor.topic, randomBlockHash())

        ethereumLogService.delete(descriptor, record)

        assertNull(findLog(collection, record.id))
    }

    @Test
    fun `save - new record`() = runBlocking {
        val newLog = randomLogRecord(topic, randomBlockHash())

        ethereumLogService.save(descriptor, listOf(newLog))

        val savedVisibleRecord = findLog(collection, newLog.id) as TestEthereumLogRecord

        assertNotNull(savedVisibleRecord)
        assertEquals(newLog.customData, savedVisibleRecord.customData)
        assertEquals(newLog.log, savedVisibleRecord.log)
    }

    @Test
    fun `save - overwrite visible`() = runBlocking {
        val blockHash = randomBlockHash()
        val transactionHash = randomWord()

        val visibleLog = randomLog(transactionHash.toString(), topic, blockHash).copy(index = 2, minorLogIndex = 3)
        val visibleRecord = randomLogRecord(visibleLog)
        // Let's change custom data in order to detect changes
        val updatedVisibleRecord = visibleRecord.copy(customData = randomString())

        saveLog(descriptor.collection, visibleRecord)
        // Here we're also checking search by index/minorIndex
        ethereumLogService.save(descriptor, listOf(updatedVisibleRecord))

        val savedVisibleRecord = findLog(collection, visibleRecord.id) as TestEthereumLogRecord

        assertNotNull(savedVisibleRecord)
        assertEquals(updatedVisibleRecord.customData, savedVisibleRecord.customData)
        assertEquals(updatedVisibleRecord.log, savedVisibleRecord.log)
    }

    @Test
    fun `save - overwrite invisible`() = runBlocking {
        val blockHash = randomBlockHash()
        val transactionHash = randomWord()

        val visibleLog = randomLog(transactionHash.toString(), topic, blockHash).copy(index = 2, minorLogIndex = 3)
        val visibleRecord = randomLogRecord(visibleLog)

        // Let's change index in order to make this record unable to be found by findVisibleByKey
        val changedVisibleLog = visibleLog.copy(index = 4)
        val updatedVisibleRecord = visibleRecord.withLog(changedVisibleLog).copy(customData = randomString())

        saveLog(descriptor.collection, visibleRecord)
        // Here we're also checking search by blockHash/logIndex
        ethereumLogService.save(descriptor, listOf(updatedVisibleRecord))

        val savedVisibleRecord = findLog(collection, visibleRecord.id) as TestEthereumLogRecord

        assertNotNull(savedVisibleRecord)
        assertEquals(updatedVisibleRecord.customData, savedVisibleRecord.customData)
        assertEquals(updatedVisibleRecord.log, savedVisibleRecord.log)
    }

    @Test
    fun `save - log record not changed`() = runBlocking {
        val log = randomLogRecord(topic, randomBlockHash())

        val savedLog = saveLog(collection, log)
        ethereumLogService.save(descriptor, listOf(log))

        val updatedLog = findLog(collection, log.id) as TestEthereumLogRecord

        assertNotNull(updatedLog)
        assertEquals(savedLog.version, updatedLog.version)
        assertEquals(log.log, updatedLog.log)
    }

    @Test
    fun `find pending logs`() = runBlocking {
        val anotherCollection = testBidSubscriber.getDescriptor().collection

        val pendingLog = saveLog(collection, randomLogRecord(topic, randomBlockHash(), Log.Status.PENDING))
        saveLog(collection, randomLogRecord(topic, randomBlockHash(), Log.Status.CONFIRMED))
        saveLog(collection, randomLogRecord(topic, randomBlockHash(), Log.Status.REVERTED))
        saveLog(anotherCollection, randomLogRecord(topic, randomBlockHash(), Log.Status.PENDING))

        val pendingLogs = ethereumLogService.findPendingLogs(descriptor).toList()

        assertEquals(1, pendingLogs.size)
        assertEquals(pendingLog, pendingLogs[0])
    }

    @Test
    fun `find and revert records`() = runBlocking {
        val anotherCollection = testBidSubscriber.getDescriptor().collection
        val blockHash = randomBlockHash()

        val reverted = saveLog(collection, randomLogRecord(topic, blockHash))
        val wrongBlockHash = saveLog(collection, randomLogRecord(topic, randomBlockHash()))
        val wrongTopic = saveLog(collection, randomLogRecord(randomWord(), blockHash))
        val wrongCollection = saveLog(anotherCollection, randomLogRecord(topic, blockHash))

        val revertedLogs = ethereumLogService.findAndRevert(descriptor, blockHash.toString()).toList()

        assertEquals(1, revertedLogs.size)
        assertEquals(reverted.id, revertedLogs[0].id)

        val savedRevertedLog = findLog(collection, reverted.id)
        assertEquals(false, savedRevertedLog!!.log!!.visible)
        assertEquals(Log.Status.REVERTED, savedRevertedLog.log!!.status)

        assertEquals(Log.Status.CONFIRMED, findLog(collection, wrongBlockHash.id)!!.log!!.status)
        assertEquals(Log.Status.CONFIRMED, findLog(collection, wrongTopic.id)!!.log!!.status)
        assertEquals(Log.Status.CONFIRMED, findLog(anotherCollection, wrongCollection.id)!!.log!!.status)
    }

    @Test
    fun `find and delete records - without status`() = runBlocking {
        val anotherCollection = testBidSubscriber.getDescriptor().collection
        val blockHash = randomBlockHash()

        val deleted = saveLog(collection, randomLogRecord(topic, blockHash))
        val wrongBlockHash = saveLog(collection, randomLogRecord(topic, randomBlockHash()))
        val wrongTopic = saveLog(collection, randomLogRecord(randomWord(), blockHash))
        val wrongCollection = saveLog(anotherCollection, randomLogRecord(topic, blockHash))

        val deletedLogs = ethereumLogService.findAndDelete(descriptor, blockHash.toString()).toList()

        assertEquals(1, deletedLogs.size)
        assertEquals(deleted.id, deletedLogs[0].id)

        assertNull(findLog(collection, deleted.id))
        assertNotNull(findLog(collection, wrongBlockHash.id))
        assertNotNull(findLog(collection, wrongTopic.id))
        assertNotNull(findLog(anotherCollection, wrongCollection.id))
    }

    @Test
    fun `find and delete records - with status`() = runBlocking {
        val blockHash = randomBlockHash()

        val deleted = saveLog(collection, randomLogRecord(topic, blockHash))
        val wrongStatus = saveLog(collection, randomLogRecord(topic, blockHash, Log.Status.PENDING))

        val deletedLogs =
            ethereumLogService.findAndDelete(descriptor, blockHash.toString(), Log.Status.CONFIRMED).toList()

        assertEquals(1, deletedLogs.size)
        assertEquals(deleted.id, deletedLogs[0].id)

        assertNull(findLog(collection, deleted.id))
        assertNotNull(findLog(collection, wrongStatus.id))
    }

    @Test
    fun `update status - record exist`() = runBlocking {
        val record = saveLog(collection, randomLogRecord(topic, randomBlockHash(), Log.Status.PENDING))

        ethereumLogService.updateStatus(descriptor, record, Log.Status.INACTIVE)

        val updatedRecord = findLog(collection, record.id)!!

        assertFalse(updatedRecord.log!!.visible)
        assertEquals(Log.Status.INACTIVE, updatedRecord.log!!.status)
    }


    @Test
    fun `update status - record doesn't exist`() = runBlocking {
        val record = randomLogRecord(topic, randomBlockHash(), Log.Status.PENDING)

        ethereumLogService.updateStatus(descriptor, record, Log.Status.INACTIVE)

        val updatedRecord = findLog(collection, record.id)!!

        assertFalse(updatedRecord.log!!.visible)
        assertEquals(Log.Status.INACTIVE, updatedRecord.log!!.status)
    }

    @Test
    fun `update status - optimistic lock handled`() = runBlocking {
        val record = saveLog(collection, randomLogRecord(topic, randomBlockHash(), Log.Status.PENDING))
        val prevVersion = record.withIdAndVersion(record.id, null)

        ethereumLogService.updateStatus(descriptor, prevVersion, Log.Status.INACTIVE)

        val updatedRecord = findLog(collection, record.id)!!

        assertFalse(updatedRecord.log!!.visible)
        assertEquals(Log.Status.INACTIVE, updatedRecord.log!!.status)
    }
}
