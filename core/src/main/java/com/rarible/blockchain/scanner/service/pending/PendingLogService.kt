package com.rarible.blockchain.scanner.service.pending

import com.rarible.blockchain.scanner.model.LogEvent
import com.rarible.blockchain.scanner.model.RichLogEvent
import com.rarible.blockchain.scanner.service.LogEventService
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import com.rarible.core.common.retryOptimisticLock
import com.rarible.core.logging.LoggingUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toFlux

class PendingLogService<OB, L : LogEvent>(
    private val logEventService: LogEventService<L>,
    private val pendingLogMarker: PendingLogMarker<OB, L>
) {

    private val logger: Logger = LoggerFactory.getLogger(PendingLogMarker::class.java)

    fun markInactive(block: OB, descriptor: LogEventDescriptor): Flux<L> {
        return LoggingUtils.withMarkerFlux { marker ->
            logEventService.findPendingLogs(descriptor.collection)
                .filter { it.topic == descriptor.topic }
                .map { RichLogEvent(it, descriptor.collection) }
                .collectList()
                .flatMapMany { pendingLogMarker.markInactive(block, it) }
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