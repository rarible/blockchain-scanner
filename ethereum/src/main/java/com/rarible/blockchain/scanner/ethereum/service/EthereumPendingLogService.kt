package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.data.RichLogEvent
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogEvent
import com.rarible.blockchain.scanner.framework.model.LogEvent
import com.rarible.blockchain.scanner.framework.service.PendingLogService
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toFlux
import scalether.domain.response.Block
import scalether.domain.response.Transaction
import scalether.java.Lists

class EthereumPendingLogService() : PendingLogService<Block<Transaction>, EthereumLogEvent> {

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