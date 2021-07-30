package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.data.LogEvent
import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import com.rarible.core.common.optimisticLock
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
class PendingLogMarker<OB, L : Log>(
    private val logService: LogService<L>,
    private val pendingLogService: PendingLogService<OB, L>
) {

    private val logger: Logger = LoggerFactory.getLogger(PendingLogService::class.java)

    suspend fun markInactive(block: OB, descriptor: LogEventDescriptor): Flow<L> {
        val pendingLogs = logService.findPendingLogs(descriptor.collection)
            .filter { it.topic == descriptor.topic }
            .map { LogEvent(it, descriptor.collection) }
            .toCollection(mutableListOf())

        return pendingLogService.markInactive(block, pendingLogs)
            .flatMapConcat { markInactive(it) }

    }

    private fun markInactive(logsToMark: LogEventStatusUpdate<L>): Flow<L> {
        val logs = logsToMark.logs
        val status = logsToMark.status
        return if (logs.isNotEmpty()) {
            logger.info("markInactive $status $logs")
            logs.asFlow().map {
                optimisticLock {
                    logService.updateStatus(it.collection, it.log, status)
                }
            }
        } else {
            emptyFlow()
        }
    }
}