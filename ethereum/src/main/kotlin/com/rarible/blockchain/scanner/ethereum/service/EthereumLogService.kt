package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogEventRepository
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.core.common.justOrEmpty
import com.rarible.core.common.toOptional
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import org.bson.types.ObjectId
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class EthereumLogService(
    private val ethereumLogEventRepository: EthereumLogEventRepository
) : LogService<EthereumLog> {

    private val logger: Logger = LoggerFactory.getLogger(EthereumLogService::class.java)

    override suspend fun delete(collection: String, log: EthereumLog): EthereumLog {
        return ethereumLogEventRepository.delete(collection, log).awaitFirst()
    }

    override suspend fun saveOrUpdate(
        collection: String,
        event: EthereumLog
    ): EthereumLog {
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

        return opt.flatMap {
            if (it.isPresent) {
                val found = it.get()
                val withCorrectId = event.copy(id = found.id, version = found.version)
                if (withCorrectId != found) {
                    logger.info("Saving changed LogEvent $withCorrectId to $collection")
                    ethereumLogEventRepository.save(collection, withCorrectId)
                } else {
                    logger.info("LogEvent didn't change: $withCorrectId")
                    found.justOrEmpty()
                }
            } else {
                logger.info("Saving new LogEvent $event")
                ethereumLogEventRepository.save(collection, event)
            }
        }.awaitFirst()
    }

    override suspend fun save(collection: String, log: EthereumLog): EthereumLog {
        return ethereumLogEventRepository.save(collection, log).awaitFirst()
    }

    override fun findPendingLogs(collection: String): Flow<EthereumLog> {
        return ethereumLogEventRepository.findPendingLogs(collection).asFlow()
    }

    override suspend fun findLogEvent(collection: String, id: ObjectId): EthereumLog {
        return ethereumLogEventRepository.findLogEvent(collection, id).awaitFirst()
    }

    override fun findAndRevert(collection: String, blockHash: String, topic: String): Flow<EthereumLog> {
        return ethereumLogEventRepository.findAndRevert(
            collection,
            Word.apply(blockHash), // TODO ???
            Word.apply(topic)  // TODO ???
        ).asFlow()
    }

    override fun findAndDelete(
        collection: String,
        blockHash: String,
        topic: String,
        status: Log.Status?
    ): Flow<EthereumLog> {
        return ethereumLogEventRepository.findAndDelete(
            collection,
            Word.apply(blockHash), // TODO ???
            Word.apply(topic),  // TODO ???
            status
        ).asFlow()
    }

    override suspend fun updateStatus(
        collection: String,
        log: EthereumLog,
        status: Log.Status
    ): EthereumLog {
        val toSave = log.copy(status = status, visible = false)
        return ethereumLogEventRepository.save(collection, toSave).awaitFirst()
    }
}