package com.rarible.blockchain.scanner.ethereum.mapper

import com.rarible.blockchain.scanner.ethereum.model.EthereumLogEvent
import com.rarible.blockchain.scanner.model.LogEvent
import com.rarible.blockchain.scanner.model.RichLogEvent
import com.rarible.blockchain.scanner.service.pending.LogEventStatusUpdate
import com.rarible.blockchain.scanner.service.pending.PendingLogMarker
import reactor.core.publisher.Flux
import scalether.domain.response.Block
import scalether.domain.response.Transaction
import scalether.java.Lists

class EthereumPendingLogMarker() : PendingLogMarker<Block<Transaction>, EthereumLogEvent> {

    override fun markInactive(
        block: Block<Transaction>,
        logs: List<RichLogEvent<EthereumLogEvent>>
    ): Flux<LogEventStatusUpdate<EthereumLogEvent>> {
        if (logs.isEmpty()) {
            return Flux.empty()
        }
        val byTxHash = logs.groupBy { it.log.transactionHash }
        val byFromNonce = logs.groupBy { Pair(it.log.from, it.log.nonce) }
        return Flux.fromIterable(Lists.toJava(block.transactions()))
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