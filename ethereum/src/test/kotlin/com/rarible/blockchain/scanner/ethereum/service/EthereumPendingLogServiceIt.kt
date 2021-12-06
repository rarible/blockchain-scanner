package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.test.AbstractIntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.IntegrationTest
import com.rarible.blockchain.scanner.ethereum.test.data.randomBlockHash
import com.rarible.blockchain.scanner.ethereum.test.data.randomLogRecord
import com.rarible.blockchain.scanner.ethereum.test.data.randomWord
import com.rarible.blockchain.scanner.framework.model.Log
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

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
    fun `find pending logs`() = runBlocking {
        val anotherCollection = testBidSubscriber.getDescriptor().collection

        val pendingLog = saveLog(collection, randomLogRecord(topic, randomBlockHash(), Log.Status.PENDING))
        saveLog(collection, randomLogRecord(topic, randomBlockHash(), Log.Status.CONFIRMED))
        saveLog(collection, randomLogRecord(topic, randomBlockHash(), Log.Status.REVERTED))
        saveLog(anotherCollection, randomLogRecord(topic, randomBlockHash(), Log.Status.PENDING))

        val pendingLogs = ethereumPendingLogService.findPendingLogs(descriptor).toList()

        assertEquals(1, pendingLogs.size)
        assertEquals(pendingLog, pendingLogs[0])
    }

    @Test
    fun `update status - record exist`() = runBlocking {
        val record = saveLog(collection, randomLogRecord(topic, randomBlockHash(), Log.Status.PENDING))

        ethereumPendingLogService.updateStatus(descriptor, record, Log.Status.INACTIVE)

        val updatedRecord = findLog(collection, record.id)!!

        assertFalse(updatedRecord.log!!.visible)
        assertEquals(Log.Status.INACTIVE, updatedRecord.log!!.status)
    }


    @Test
    fun `update status - record doesn't exist`() = runBlocking {
        val record = randomLogRecord(topic, randomBlockHash(), Log.Status.PENDING)

        ethereumPendingLogService.updateStatus(descriptor, record, Log.Status.INACTIVE)

        val updatedRecord = findLog(collection, record.id)!!

        assertFalse(updatedRecord.log!!.visible)
        assertEquals(Log.Status.INACTIVE, updatedRecord.log!!.status)
    }

    @Test
    fun `update status - optimistic lock handled`() = runBlocking {
        val record = saveLog(collection, randomLogRecord(topic, randomBlockHash(), Log.Status.PENDING))
        val prevVersion = record.withIdAndVersion(record.id, null)

        ethereumPendingLogService.updateStatus(descriptor, prevVersion, Log.Status.INACTIVE)

        val updatedRecord = findLog(collection, record.id)!!

        assertFalse(updatedRecord.log!!.visible)
        assertEquals(Log.Status.INACTIVE, updatedRecord.log!!.status)
    }
}