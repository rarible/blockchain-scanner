package com.rarible.blockchain.scanner.processor

import com.rarible.blockchain.scanner.mapper.LogEventMapper
import com.rarible.blockchain.scanner.model.EventData
import com.rarible.blockchain.scanner.model.LogEvent
import com.rarible.blockchain.scanner.service.LogEventService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.common.retryOptimisticLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux

class LogEventProcessor<OB, OL, L : LogEvent, D : EventData>(
    val subscriber: LogEventSubscriber<OL, OB, D>,
    private val logEventMapper: LogEventMapper<OL, OB, L>,
    private val logEventService: LogEventService<L>,
    private val logEventListeners: List<LogEventListener<L>>
) {

    private val logger: Logger = LoggerFactory.getLogger(subscriber.javaClass)

    init {
        logger.info(
            "Creating LogEventProcessor for ${subscriber.javaClass.simpleName}," +
                    " got onLogEventListeners: ${logEventListeners.joinToString { it.javaClass.simpleName }}"
        )
    }

    fun processLogs(marker: Marker, block: OB, logs: List<OL>): Flux<L> {
        if (logs.isNotEmpty()) {
            logger.info(marker, "processLogs ${logs.size} logs")
        }
        val processedLogs = Flux.fromIterable(logs.withIndex()).flatMap { (idx, log) ->
            onLog(marker, block, idx, log)
        }
        return processedLogs.flatMap { notifyListeners(it) }
    }

    private fun onLog(marker: Marker, block: OB, index: Int, log: OL): Flux<L> {
        logger.info(marker, "onLog $log")

        val mappedLogs = subscriber.map(log, block).toFlux()
            .collectList()
            .flatMapIterable { dataCollection ->
                dataCollection.mapIndexed { minorLogIndex, data ->
                    logEventMapper.map(
                        block,
                        log,
                        index,
                        minorLogIndex,
                        data,
                        subscriber.getDescriptor()
                    )
                }
            }
        return saveProcessedLogs(marker, mappedLogs)
    }

    private fun saveProcessedLogs(marker: Marker, logs: Flux<L>): Flux<L> {
        return logs.flatMap {
            Mono.just(it)
                .flatMap { toSave ->
                    logger.info(marker, "saving $toSave to ${subscriber.collection}")
                    logEventService.saveOrUpdate(marker, subscriber.collection, it)
                }.retryOptimisticLock(3)
        }
    }

    private fun notifyListeners(logEvent: L): Mono<L> {
        return Flux.concat(
            logEventListeners.map { it.onLogEvent(logEvent) }
        ).then(Mono.just(logEvent))
    }

}