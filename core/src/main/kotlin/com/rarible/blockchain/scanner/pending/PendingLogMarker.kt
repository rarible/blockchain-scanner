package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.data.LogEvent
import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogEventDescriptor
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.core.common.optimisticLock
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
class PendingLogMarker<BB, L : Log, D : LogEventDescriptor>(
    private val logService: LogService<L, D>,
    private val pendingLogService: PendingLogService<BB, L, D>
) {

    private val logger: Logger = LoggerFactory.getLogger(PendingLogService::class.java)

    suspend fun markInactive(block: BB, descriptor: D): Flow<L> {
        val pendingLogs = logService.findPendingLogs(descriptor)
            .filter { it.topic == descriptor.topic }
            .map { LogEvent(it, descriptor) }
            .toCollection(mutableListOf())

        return pendingLogService.markInactive(block, pendingLogs)
            .flatMapConcat { markInactive(it) }

    }

    private fun markInactive(logsToMark: LogEventStatusUpdate<L, D>): Flow<L> {
        val logs = logsToMark.logs
        val status = logsToMark.status
        return if (logs.isNotEmpty()) {
            logger.info("markInactive $status $logs")
            logs.asFlow().map {
                optimisticLock {
                    logService.updateStatus(it.descriptor, it.log, status)
                }
            }
        } else {
            emptyFlow()
        }
    }
}