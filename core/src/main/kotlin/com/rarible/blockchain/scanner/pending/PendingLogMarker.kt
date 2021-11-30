package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.flow.toList
import org.slf4j.LoggerFactory

@FlowPreview
class PendingLogMarker<L : Log, R : LogRecord<L, *>, D : Descriptor>(
    private val logService: LogService<L, R, D>,
    private val pendingLogService: PendingLogService<L, R, D>
) {

    private val logger = LoggerFactory.getLogger(PendingLogService::class.java)

    suspend fun markInactive(blockHash: String, descriptor: D): List<R> {
        val pendingLogs = logService.findPendingLogs(descriptor)
            .map { LogEvent(it, descriptor) }
            .toCollection(mutableListOf())

        return pendingLogService.getInactive(blockHash, pendingLogs).toList()
            .flatMap { markInactive(it) }.toList()
    }

    private suspend fun markInactive(logsToMark: LogEventStatusUpdate<L, R, D>): List<R> {
        val logs = logsToMark.logs
        val status = logsToMark.status
        return if (logs.isNotEmpty()) {
            logs.map {
                logger.info("Marking with status '{}' log record: [{}]", status, it.record)
                logService.updateStatus(it.descriptor, it.record, status)
            }
        } else {
            emptyList()
        }
    }
}
