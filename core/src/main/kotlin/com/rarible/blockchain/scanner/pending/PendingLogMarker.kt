package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.data.LogEvent
import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import com.rarible.blockchain.scanner.util.flatten
import com.rarible.core.common.optimisticLock
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
class PendingLogMarker<BB, L : Log>(
    private val logService: LogService<L>,
    private val pendingLogService: PendingLogService<BB, L>
) {

    private val logger: Logger = LoggerFactory.getLogger(PendingLogService::class.java)

    fun markInactive(block: BB, descriptor: LogEventDescriptor): Flow<L> = flatten {
        val pendingLogs = logService.findPendingLogs(descriptor.collection)
            .filter { it.topic == descriptor.topic }
            .map { LogEvent(it, descriptor.collection) }
            .toCollection(mutableListOf())

        pendingLogService.markInactive(block, pendingLogs)
            .flatMapConcat { markInactive(it) }

    }

    private fun markInactive(logsToMark: LogEventStatusUpdate<L>): Flow<L> {
        val logs = logsToMark.logs
        val status = logsToMark.status
        return if (logs.isNotEmpty()) {
            logger.info("markInactive $status $logs")
            logs.asFlow().map {
                optimisticLock {
                    //todo optimistic lock не особо поможет, потому что нет повторного чтения логов
                    logService.updateStatus(it.collection, it.log, status)
                }
            }
        } else {
            emptyFlow()
        }
    }
}