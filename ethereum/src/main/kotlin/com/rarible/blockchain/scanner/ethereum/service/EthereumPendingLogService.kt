package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.data.LogEvent
import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toFlux
import scalether.core.MonoEthereum
import scalether.java.Lists

@Component
class EthereumPendingLogService(
    private val monoEthereum: MonoEthereum
) : PendingLogService<EthereumBlockchainBlock, EthereumLog> {

    override fun markInactive(
        block: EthereumBlockchainBlock,
        logs: List<LogEvent<EthereumLog>>
    ): Flux<LogEventStatusUpdate<EthereumLog>> {
        if (logs.isEmpty()) {
            return Flux.empty()
        }
        val byTxHash = logs.groupBy { it.log.transactionHash }
        val byFromNonce = logs.groupBy { Pair(it.log.from, it.log.nonce) }
        val fullBlock = monoEthereum.ethGetFullBlockByHash(block.ethBlock.hash())
        return fullBlock.flatMapMany { Lists.toJava(it.transactions()).toFlux() }
            .flatMap { tx ->
                val first = byTxHash[tx.hash().toString()] ?: emptyList() // TODO ???
                val second =
                    (byFromNonce[Pair(tx.from().hex(), tx.nonce().toLong())] ?: emptyList()) - first // TODO ???
                listOf(
                    LogEventStatusUpdate(first, Log.Status.INACTIVE),
                    LogEventStatusUpdate(second, Log.Status.DROPPED)
                ).toFlux()
            }
    }
}