package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogEventDescriptor
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
) : LogService<EthereumLog, EthereumLogEventDescriptor> {

    private val logger: Logger = LoggerFactory.getLogger(EthereumLogService::class.java)

    override suspend fun delete(descriptor: EthereumLogEventDescriptor, log: EthereumLog): EthereumLog {
        return ethereumLogEventRepository.delete(descriptor.collection, log).awaitFirst()
    }

    override suspend fun saveOrUpdate(
        descriptor: EthereumLogEventDescriptor,
        event: EthereumLog
    ): EthereumLog {
        val collection = descriptor.collection
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

    override suspend fun save(descriptor: EthereumLogEventDescriptor, log: EthereumLog): EthereumLog {
        return ethereumLogEventRepository.save(descriptor.collection, log).awaitFirst()
    }

    override fun findPendingLogs(descriptor: EthereumLogEventDescriptor): Flow<EthereumLog> {
        return ethereumLogEventRepository.findPendingLogs(descriptor.collection).asFlow()
    }

    override suspend fun findLogEvent(descriptor: EthereumLogEventDescriptor, id: ObjectId): EthereumLog {
        return ethereumLogEventRepository.findLogEvent(descriptor.collection, id).awaitFirst()
    }

    override fun findAndRevert(descriptor: EthereumLogEventDescriptor, blockHash: String): Flow<EthereumLog> {
        return ethereumLogEventRepository.findAndRevert(
            descriptor.collection,
            Word.apply(blockHash), // TODO ???
            Word.apply(descriptor.topic)  // TODO ???
        ).asFlow()
    }

    override fun findAndDelete(
        descriptor: EthereumLogEventDescriptor,
        blockHash: String,
        status: Log.Status?
    ): Flow<EthereumLog> {
        return ethereumLogEventRepository.findAndDelete(
            descriptor.collection,
            Word.apply(blockHash), // TODO ???
            Word.apply(descriptor.topic),  // TODO ???
            status
        ).asFlow()
    }

    override suspend fun updateStatus(
        descriptor: EthereumLogEventDescriptor,
        log: EthereumLog,
        status: Log.Status
    ): EthereumLog {
        val toSave = log.copy(status = status, visible = false)
        return ethereumLogEventRepository.save(descriptor.collection, toSave).awaitFirst()
    }
}