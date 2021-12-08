package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.model.EthereumPendingLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumPendingLogStatusUpdate
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.core.common.optimisticLock
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.kotlin.core.publisher.toFlux
import scalether.core.MonoEthereum
import scalether.java.Lists

@Component
class EthereumPendingLogService(
    private val ethereumLogRepository: EthereumLogRepository,
    private val properties: EthereumScannerProperties,
    private val monoEthereum: MonoEthereum
) {

    private val logger = LoggerFactory.getLogger(EthereumPendingLogService::class.java)

    suspend fun markInactive(blockHash: String, descriptor: EthereumDescriptor): List<EthereumLogRecord<*>> {
        val pendingLogs = findPendingLogs(descriptor)
            .map { EthereumPendingLog(it, descriptor) }
            .toCollection(mutableListOf())

        return getInactive(blockHash, pendingLogs).toList()
            .flatMap { markInactive(it) }.toList()
    }

    fun findPendingLogs(descriptor: EthereumDescriptor): Flow<EthereumLogRecord<*>> {
        return ethereumLogRepository.findPendingLogs(descriptor.entityType, descriptor.collection, descriptor.ethTopic)
            .asFlow()
    }

    private fun getInactive(
        blockHash: String,
        records: List<EthereumPendingLog>
    ): Flow<EthereumPendingLogStatusUpdate> {
        if (records.isEmpty()) {
            return emptyFlow()
        }

        val byTxHash = records.groupBy { it.record.log.transactionHash }
        val fullBlock = monoEthereum.ethGetFullBlockByHash(Word.apply(blockHash))
        return fullBlock.flatMapMany { Lists.toJava(it.transactions()).toFlux() }
            .map { tx ->
                val first = byTxHash[tx.hash().toString()] ?: emptyList()
                EthereumPendingLogStatusUpdate(first, Log.Status.INACTIVE)
            }.asFlow()
    }

    private suspend fun markInactive(
        logsToMark: EthereumPendingLogStatusUpdate
    ): List<EthereumLogRecord<*>> {
        val logs = logsToMark.logs
        val status = logsToMark.status
        return if (logs.isNotEmpty()) {
            logs.map {
                updateStatus(it.descriptor, it.record, status)
            }
        } else {
            emptyList()
        }
    }

    suspend fun updateStatus(
        descriptor: EthereumDescriptor,
        record: EthereumLogRecord<*>,
        status: Log.Status
    ): EthereumLogRecord<*> = optimisticLock(properties.optimisticLockRetries) {
        val exist = ethereumLogRepository.findLogEvent(descriptor.entityType, descriptor.collection, record.id)

        val copy = exist?.withIdAndVersion(record.id, exist.version)
            ?: record.withIdAndVersion(record.id, null)

        logger.info("Updating status {} -> {} for record: {}", copy.log.status, status, copy)
        val updatedCopy = copy.withLog(record.log.copy(status = status, visible = false))
        ethereumLogRepository.save(descriptor.collection, updatedCopy)
    }
}
