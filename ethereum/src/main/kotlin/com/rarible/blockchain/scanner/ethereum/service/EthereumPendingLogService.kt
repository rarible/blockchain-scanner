package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.data.RichLogEvent
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogEvent
import com.rarible.blockchain.scanner.framework.model.LogEvent
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toFlux
import scalether.core.MonoEthereum
import scalether.java.Lists

class EthereumPendingLogService(
    private val monoEthereum: MonoEthereum
) : PendingLogService<EthereumBlockchainBlock, EthereumLogEvent> {

    override fun markInactive(
        block: EthereumBlockchainBlock,
        logs: List<RichLogEvent<EthereumLogEvent>>
    ): Flux<LogEventStatusUpdate<EthereumLogEvent>> {
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
                    LogEventStatusUpdate(first, LogEvent.Status.INACTIVE),
                    LogEventStatusUpdate(second, LogEvent.Status.DROPPED)
                ).toFlux()
            }
    }
}