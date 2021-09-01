package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.configuration.AbstractIntegrationTest
import com.rarible.blockchain.scanner.test.configuration.IntegrationTest
import com.rarible.blockchain.scanner.test.data.randomBlockchainBlock
import com.rarible.blockchain.scanner.test.data.randomTestLogRecord
import com.rarible.blockchain.scanner.test.data.testDescriptor1
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import com.rarible.blockchain.scanner.test.service.TestPendingLogService
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@FlowPreview
@IntegrationTest
class PendingLogMarkerIt : AbstractIntegrationTest() {

    private val descriptor = testDescriptor1()
    private val topic = descriptor.topic
    private val collection = descriptor.collection

    @Test
    fun `mark inactive`() = runBlocking {
        val block = randomBlockchainBlock()

        val logToBeInactivated = saveLog(collection, randomTestLogRecord(topic, block.hash, Log.Status.PENDING))
        val logToBeDropped = saveLog(collection, randomTestLogRecord(topic, block.hash, Log.Status.PENDING))
        val logToBeSkipped = saveLog(collection, randomTestLogRecord(topic, block.hash, Log.Status.PENDING))
        val confirmedLog = saveLog(collection, randomTestLogRecord(topic, block.hash, Log.Status.CONFIRMED))

        val pendingLogService = TestPendingLogService(
            listOf(logToBeDropped.log!!.transactionHash),
            listOf(logToBeInactivated.log!!.transactionHash)
        )

        val marker = createMarker(pendingLogService)

        marker.markInactive(block, descriptor).toList()

        // LogRecords specified in PendingLogService should be dropped/deactivated, other LogRecords should be the same
        assertEquals(Log.Status.DROPPED, findLog(collection, logToBeDropped.id)!!.log!!.status)
        assertEquals(Log.Status.INACTIVE, findLog(collection, logToBeInactivated.id)!!.log!!.status)
        assertEquals(Log.Status.PENDING, findLog(collection, logToBeSkipped.id)!!.log!!.status)
        assertEquals(Log.Status.CONFIRMED, findLog(collection, confirmedLog.id)!!.log!!.status)
    }

    @Test
    fun `mark inactive - nothing found, nothing changed`() = runBlocking {
        val block = randomBlockchainBlock()
        val pendingLog = saveLog(collection, randomTestLogRecord(topic, block.hash, Log.Status.PENDING))

        val pendingLogService = TestPendingLogService()
        val marker = createMarker(pendingLogService)

        marker.markInactive(block, descriptor).toList()

        // No LogRecords found to update status - nothing should be updated
        assertEquals(Log.Status.PENDING, findLog(collection, pendingLog.id)!!.log!!.status)
    }

    private fun createMarker(
        pendingLogService: TestPendingLogService
    ): PendingLogMarker<TestBlockchainBlock, TestLog, TestLogRecord<*>, TestDescriptor> {
        return PendingLogMarker(
            logService = testLogService,
            pendingLogService = pendingLogService
        )
    }

}
