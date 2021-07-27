package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.mapper.LogMapper
import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.blockchain.scanner.subscriber.LogEventListener
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import com.rarible.core.common.retryOptimisticLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux

class LogEventHandler<OB, OL, L : Log, D : EventData>(
    val subscriber: LogEventSubscriber<OL, OB, D>,
    private val logMapper: LogMapper<OL, OB, L>,
    private val logService: LogService<L>,
    private val logEventListeners: List<LogEventListener<L>>
) {

    private val logger: Logger = LoggerFactory.getLogger(subscriber.javaClass)

    init {
        logger.info(
            "Creating LogEventProcessor for ${subscriber.javaClass.simpleName}," +
                    " got onLogEventListeners: ${logEventListeners.joinToString { it.javaClass.simpleName }}"
        )
    }

    fun beforeHandleBlock(event: BlockEvent): Flux<L> {
        val collection = subscriber.getDescriptor().collection
        val topic = subscriber.getDescriptor().topic

        return if (event.reverted != null) {
            logService
                .findAndDelete(collection, event.block.hash, topic, Log.Status.REVERTED)
                .thenMany(logService.findAndRevert(collection, topic, event.reverted.hash))
        } else {
            logService.findAndDelete(collection, event.block.hash, topic, Log.Status.REVERTED)
                .thenMany(Flux.empty())
        }
    }

    fun handleLogs(marker: Marker, block: OB, logs: List<OL>): Flux<L> {
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

        val mappedLogs = subscriber.getEventData(log, block).toFlux()
            .collectList()
            .flatMapIterable { dataCollection ->
                dataCollection.mapIndexed { minorLogIndex, data ->
                    logMapper.map(
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
        val collection = subscriber.getDescriptor().collection
        return logs.flatMap {
            Mono.just(it)
                .flatMap { toSave ->
                    logger.info(marker, "saving $toSave to $collection")
                    logService.saveOrUpdate(marker, collection, it)
                }.retryOptimisticLock(3)
        }
    }

    private fun notifyListeners(logEvent: L): Mono<L> {
        return Flux.concat(
            logEventListeners.map { it.onLogEvent(logEvent) }
        ).then(Mono.just(logEvent))
    }

}