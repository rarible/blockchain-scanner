package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogEventRepository
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.core.common.justOrEmpty
import com.rarible.core.common.toOptional
import io.daonomic.rpc.domain.Word
import org.bson.types.ObjectId
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Component
class EthereumLogService(
    private val ethereumLogEventRepository: EthereumLogEventRepository
) : LogService<EthereumLog> {

    private val logger: Logger = LoggerFactory.getLogger(EthereumLogService::class.java)

    override fun delete(collection: String, log: EthereumLog): Mono<EthereumLog> {
        return ethereumLogEventRepository.delete(collection, log)
    }

    override fun saveOrUpdate(
        marker: Marker,
        collection: String,
        event: EthereumLog
    ): Mono<EthereumLog> {
        val opt = ethereumLogEventRepository.findVisibleByKey(
            collection,
            Word.apply(event.transactionHash), // TODO ???
            Word.apply(event.topic), // TODO ???
            event.index,
            event.minorLogIndex
        ).switchIfEmpty(
            ethereumLogEventRepository.findByKey(
                collection,
                Word.apply(event.transactionHash), // TODO ???
                event.blockHash!!,
                event.logIndex!!,
                event.minorLogIndex
            )
        ).toOptional()

        return opt.flatMap { it ->
            if (it.isPresent) {
                val found = it.get()
                val withCorrectId = event.copy(id = found.id, version = found.version)
                if (withCorrectId != found) {
                    logger.info(
                        marker,
                        "Saving changed LogEvent $withCorrectId to $collection"
                    )
                    ethereumLogEventRepository.save(collection, withCorrectId)
                } else {
                    logger.info(marker, "LogEvent didn't change: $withCorrectId")
                    found.justOrEmpty()
                }
            } else {
                logger.info(marker, "Saving new LogEvent $event")
                ethereumLogEventRepository.save(collection, event)
            }
        }
    }

    override fun save(collection: String, log: EthereumLog): Mono<EthereumLog> {
        return ethereumLogEventRepository.save(collection, log)
    }

    override fun findPendingLogs(collection: String): Flux<EthereumLog> {
        return ethereumLogEventRepository.findPendingLogs(collection)
    }

    override fun findLogEvent(collection: String, id: ObjectId): Mono<EthereumLog> {
        return ethereumLogEventRepository.findLogEvent(collection, id)
    }

    override fun findAndRevert(collection: String, blockHash: String, topic: String): Flux<EthereumLog> {
        return ethereumLogEventRepository.findAndRevert(
            collection,
            Word.apply(blockHash), // TODO ???
            Word.apply(topic)  // TODO ???
        )
    }

    override fun findAndDelete(
        collection: String,
        blockHash: String,
        topic: String,
        status: Log.Status?
    ): Flux<EthereumLog> {
        return ethereumLogEventRepository.findAndDelete(
            collection,
            Word.apply(blockHash), // TODO ???
            Word.apply(topic),  // TODO ???
            status
        )
    }

    override fun updateStatus(
        collection: String,
        log: EthereumLog,
        status: Log.Status
    ): Mono<EthereumLog> {
        val toSave = log.copy(status = status, visible = false)
        return ethereumLogEventRepository.save(collection, toSave)
    }
}