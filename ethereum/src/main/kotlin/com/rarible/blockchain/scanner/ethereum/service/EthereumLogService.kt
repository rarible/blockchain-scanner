package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogEventRepository
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.LogService
import com.rarible.core.common.justOrEmpty
import com.rarible.core.common.toOptional
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class EthereumLogService(
    private val ethereumLogEventRepository: EthereumLogEventRepository
) : LogService<EthereumLog, EthereumLogRecord<*>, EthereumDescriptor> {

    private val logger: Logger = LoggerFactory.getLogger(EthereumLogService::class.java)

    override suspend fun delete(descriptor: EthereumDescriptor, record: EthereumLogRecord<*>): EthereumLogRecord<*> {
        return ethereumLogEventRepository.delete(descriptor.collection, record).awaitFirst()
    }

    override suspend fun saveOrUpdate(
        descriptor: EthereumDescriptor,
        record: EthereumLogRecord<*>
    ): EthereumLogRecord<*> {
        val collection = descriptor.collection
        val log = record.log!!
        val opt = ethereumLogEventRepository.findVisibleByKey(
            collection,
            Word.apply(log.transactionHash), // TODO ???
            Word.apply(log.topic), // TODO ???
            log.index,
            log.minorLogIndex
        ).switchIfEmpty(
            ethereumLogEventRepository.findByKey(
                collection,
                Word.apply(log.transactionHash), // TODO ???
                log.blockHash!!,
                log.logIndex!!,
                log.minorLogIndex
            )
        ).toOptional()

        return opt.flatMap {
            if (it.isPresent) {
                val found = it.get()
                val withCorrectId = record.withIdAndVersion(found.id, found.version)
                if (withCorrectId != found) {
                    logger.info("Saving changed LogEvent $withCorrectId to $collection")
                    ethereumLogEventRepository.save(collection, withCorrectId)
                } else {
                    logger.info("LogEvent didn't change: $withCorrectId")
                    found.justOrEmpty()
                }
            } else {
                logger.info("Saving new LogEvent $record")
                ethereumLogEventRepository.save(collection, record)
            }
        }.awaitFirst()
    }

    override suspend fun save(descriptor: EthereumDescriptor, record: EthereumLogRecord<*>): EthereumLogRecord<*> {
        return ethereumLogEventRepository.save(descriptor.collection, record).awaitFirst()
    }

    override fun findPendingLogs(descriptor: EthereumDescriptor): Flow<EthereumLogRecord<*>> {
        return ethereumLogEventRepository.findPendingLogs(descriptor.collection, descriptor.topic).asFlow()
    }

    override fun findAndRevert(descriptor: EthereumDescriptor, blockHash: String): Flow<EthereumLogRecord<*>> {
        return ethereumLogEventRepository.findAndRevert(
            descriptor.collection,
            Word.apply(blockHash), // TODO ???
            descriptor.topic
        ).asFlow()
    }

    override fun findAndDelete(
        descriptor: EthereumDescriptor,
        blockHash: String,
        status: Log.Status?
    ): Flow<EthereumLogRecord<*>> {
        return ethereumLogEventRepository.findAndDelete(
            descriptor.collection,
            Word.apply(blockHash), // TODO ???
            descriptor.topic,
            status
        ).asFlow()
    }

    override suspend fun updateStatus(
        descriptor: EthereumDescriptor,
        record: EthereumLogRecord<*>,
        status: Log.Status
    ): EthereumLogRecord<*> {
        val copy = record.withLog(record.log!!.copy(status = status, visible = false))
        return ethereumLogEventRepository.save(descriptor.collection, copy).awaitFirst()
    }
}