package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.LogEventStatusUpdate
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
            .map { LogEvent(it, descriptor) }
            .toCollection(mutableListOf())

        return getInactive(blockHash, pendingLogs).toList()
            .flatMap { markInactive(it) }.toList()
    }

    fun findPendingLogs(descriptor: EthereumDescriptor): Flow<EthereumLogRecord<*>> {
        return ethereumLogRepository.findPendingLogs(descriptor.collection, descriptor.topic).asFlow()
    }

    private fun getInactive(
        blockHash: String,
        records: List<LogEvent<EthereumLog, EthereumLogRecord<*>, EthereumDescriptor>>
    ): Flow<LogEventStatusUpdate<EthereumLog, EthereumLogRecord<*>, EthereumDescriptor>> {
        if (records.isEmpty()) {
            return emptyFlow()
        }
        val byTxHash = records.groupBy { it.record.log!!.transactionHash }
        val byFromNonce = records.groupBy { Pair(it.record.log!!.from, it.record.log!!.nonce) }
        val fullBlock = monoEthereum.ethGetFullBlockByHash(Word.apply(blockHash))
        return fullBlock.flatMapMany { Lists.toJava(it.transactions()).toFlux() }
            .flatMap { tx ->
                val first = byTxHash[tx.hash().toString()] ?: emptyList()
                val second = (byFromNonce[Pair(tx.from(), tx.nonce().toLong())] ?: emptyList()) - first
                listOf(
                    LogEventStatusUpdate(first, Log.Status.INACTIVE),
                    LogEventStatusUpdate(second, Log.Status.DROPPED)
                ).toFlux()
            }.asFlow()
    }

    private suspend fun markInactive(
        logsToMark: LogEventStatusUpdate<EthereumLog, EthereumLogRecord<*>, EthereumDescriptor>
    ): List<EthereumLogRecord<*>> {
        val logs = logsToMark.logs
        val status = logsToMark.status
        return if (logs.isNotEmpty()) {
            logs.map {
                logger.info("Marking with status '{}' log record: [{}]", status, it.record)
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
        val exist = ethereumLogRepository.findLogEvent(descriptor.collection, record.id)

        val copy = exist?.withIdAndVersion(record.id, exist.version)
            ?: record.withIdAndVersion(record.id, null)

        val updatedCopy = copy.withLog(record.log!!.copy(status = status, visible = false))
        ethereumLogRepository.save(descriptor.collection, updatedCopy)
    }
}
