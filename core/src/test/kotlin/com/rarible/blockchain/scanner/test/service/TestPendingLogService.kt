package com.rarible.blockchain.scanner.test.service

import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.test.client.TestBlockchainBlock
import com.rarible.blockchain.scanner.test.model.TestDescriptor
import com.rarible.blockchain.scanner.test.model.TestLog
import com.rarible.blockchain.scanner.test.model.TestLogRecord
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf

class TestPendingLogService(
    private val droppedLogs: List<String> = emptyList(),
    private val inactiveLogs: List<String> = emptyList()
) : PendingLogService<TestBlockchainBlock, TestLog, TestLogRecord<*>, TestDescriptor> {

    @Suppress("PARAMETER_NAME_CHANGED_ON_OVERRIDE")
    override fun getInactive(
        block: TestBlockchainBlock,
        logs: List<LogEvent<TestLog, TestLogRecord<*>, TestDescriptor>>
    ): Flow<LogEventStatusUpdate<TestLog, TestLogRecord<*>, TestDescriptor>> {
        val droppedLogs = logs.filter { droppedLogs.contains(it.record.log!!.transactionHash) }
        val inactiveLogs = logs.filter { inactiveLogs.contains(it.record.log!!.transactionHash) }
        return flowOf(
            LogEventStatusUpdate(droppedLogs, Log.Status.DROPPED),
            LogEventStatusUpdate(inactiveLogs, Log.Status.INACTIVE)
        )

    }
}
