package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.data.LogEvent
import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.util.flatten
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory

@FlowPreview
class PendingLogMarker<BB : BlockchainBlock, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    private val logService: LogService<L, R, D>,
    private val pendingLogService: PendingLogService<BB, L, R, D>
) {

    private val logger = LoggerFactory.getLogger(PendingLogService::class.java)

    fun markInactive(block: BB, descriptor: D): Flow<R> = flatten {
        val pendingLogs = logService.findPendingLogs(descriptor)
            .map { LogEvent(it, descriptor) }
            .toCollection(mutableListOf())

        pendingLogService.getInactive(block, pendingLogs)
            .flatMapConcat { markInactive(it) }
    }

    private fun markInactive(logsToMark: LogEventStatusUpdate<L, R, D>): Flow<R> {
        val logs = logsToMark.logs
        val status = logsToMark.status
        return if (logs.isNotEmpty()) {
            logger.info("Marking with status '{}' {} log records", status, logs.size)
            logs.asFlow().map {
                logger.info("Marking with status '{}' log record: [{}]", status, it.record)
                logService.updateStatus(it.descriptor, it.record, status)
            }
        } else {
            emptyFlow()
        }
    }
}