package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.data.RichLogEvent
import com.rarible.blockchain.scanner.framework.model.LogEvent
import com.rarible.blockchain.scanner.framework.service.LogEventService
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import com.rarible.core.common.retryOptimisticLock
import com.rarible.core.logging.LoggingUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toFlux

class PendingLogMarker<OB, L : LogEvent>(
    private val logEventService: LogEventService<L>,
    private val pendingLogService: PendingLogService<OB, L>
) {

    private val logger: Logger = LoggerFactory.getLogger(PendingLogService::class.java)

    fun markInactive(block: OB, descriptor: LogEventDescriptor): Flux<L> {
        return LoggingUtils.withMarkerFlux { marker ->
            logEventService.findPendingLogs(descriptor.collection)
                .filter { it.topic == descriptor.topic }
                .map { RichLogEvent(it, descriptor.collection) }
                .collectList()
                .flatMapMany { pendingLogService.markInactive(block, it) }
                .flatMap { markInactive(marker, it) }
        }
    }

    private fun markInactive(marker: Marker, logsToMark: LogEventStatusUpdate<L>): Flux<L> {
        val logs = logsToMark.logs
        val status = logsToMark.status
        return if (logs.isNotEmpty()) {
            logger.info(marker, "markInactive $status $logs")
            logs.toFlux().flatMap {
                logEventService.updateStatus(it.collection, it.log, status)
                    .retryOptimisticLock()
            }
        } else {
            Flux.empty()
        }
    }
}