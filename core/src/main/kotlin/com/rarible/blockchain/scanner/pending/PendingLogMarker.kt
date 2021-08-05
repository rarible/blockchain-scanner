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
import com.rarible.core.common.optimisticLock
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
class PendingLogMarker<BB : BlockchainBlock, L : Log, R : LogRecord<L, *>, D : Descriptor>(
    private val logService: LogService<L, R, D>,
    private val pendingLogService: PendingLogService<BB, L, R, D>
) {

    private val logger: Logger = LoggerFactory.getLogger(PendingLogService::class.java)

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
            logger.info("Marking with status {} next logs: {}", status, logs)
            logs.asFlow().map {
                optimisticLock {
                    //todo optimistic lock не особо поможет, потому что нет повторного чтения логов
                    logService.updateStatus(it.descriptor, it.record, status)
                }
            }
        } else {
            emptyFlow()
        }
    }
}