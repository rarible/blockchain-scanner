package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.reactive.asFlow
import org.springframework.stereotype.Component
import reactor.kotlin.core.publisher.toFlux
import scalether.core.MonoEthereum
import scalether.java.Lists

@Component
class EthereumPendingLogService(
    private val monoEthereum: MonoEthereum
) : PendingLogService<EthereumBlockchainBlock, EthereumLog, EthereumLogRecord<*>, EthereumDescriptor> {

    override fun getInactive(
        block: EthereumBlockchainBlock,
        records: List<LogEvent<EthereumLog, EthereumLogRecord<*>, EthereumDescriptor>>
    ): Flow<LogEventStatusUpdate<EthereumLog, EthereumLogRecord<*>, EthereumDescriptor>> {
        if (records.isEmpty()) {
            return emptyFlow()
        }
        val byTxHash = records.groupBy { it.record.log!!.transactionHash }
        val byFromNonce = records.groupBy { Pair(it.record.log!!.from, it.record.log!!.nonce) }
        val fullBlock = monoEthereum.ethGetFullBlockByHash(block.ethBlock.hash())
        return fullBlock.flatMapMany { Lists.toJava(it.transactions()).toFlux() }
            .flatMap { tx ->
                val first = byTxHash[tx.hash().toString()] ?: emptyList()
                val second =
                    (byFromNonce[Pair(tx.from(), tx.nonce().toLong())] ?: emptyList()) - first
                listOf(
                    LogEventStatusUpdate(first, Log.Status.INACTIVE),
                    LogEventStatusUpdate(second, Log.Status.DROPPED)
                ).toFlux()
            }.asFlow()
    }
}
