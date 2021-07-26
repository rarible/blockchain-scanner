package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.client.BlockchainClient
import com.rarible.blockchain.scanner.model.BlockLogs
import com.rarible.blockchain.scanner.model.BlockMeta
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import io.daonomic.rpc.domain.Word
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.RetryBackoffSpec
import scalether.core.MonoEthereum
import scalether.domain.Address
import scalether.domain.request.LogFilter
import scalether.domain.request.TopicFilter
import scalether.domain.response.Block
import scalether.domain.response.Log
import scalether.domain.response.Transaction
import scalether.util.Hex
import java.math.BigInteger

class EthereumClient(
    private val ethereum: MonoEthereum,
    private val backoff: RetryBackoffSpec
) : BlockchainClient<Block<Transaction>, Log> {

    private val logger: Logger = LoggerFactory.getLogger(EthereumClient::class.java)

    override fun getFullBlock(hash: String): Mono<Block<Transaction>> {
        return ethereum.ethGetFullBlockByHash(Word.apply(hash))
    }

    override fun getBlockMeta(id: Long): Mono<BlockMeta> {
        return ethereum.ethGetBlockByNumber(BigInteger.valueOf(id)).map {
            BlockMeta(
                id,
                it.hash().toString(),
                it.timestamp().toLong()
            )
        }
    }

    override fun getBlockEvents(
        block: Block<Transaction>,
        descriptor: LogEventDescriptor,
        marker: Marker
    ): Mono<List<Log>> {
        val filter = LogFilter
            .apply(TopicFilter.simple(Word.apply(descriptor.topic))) // TODO ???
            .address(*descriptor.contracts.map { Address.apply(it) }.toTypedArray())
            .blockHash(block.hash()) // TODO ???

        return ethereum.ethGetLogsJava(filter)
            .map { orderByTransaction(it) }
            .doOnError { logger.warn(marker, "Unable to get logs for block ${block.hash()}", it) }
            .retryWhen(backoff)
    }

    override fun getBlockEvents(
        descriptor: LogEventDescriptor,
        range: LongRange,
        marker: Marker
    ): Flux<BlockLogs<Log>> {

        val addresses = descriptor.contracts.map { Address.apply(it) }
        val filter = LogFilter
            .apply(TopicFilter.simple(Word.apply(descriptor.topic))) // TODO ???
            .address(*addresses.toTypedArray())
        val finalFilter = filter.blocks(
            BigInteger.valueOf(range.first).encodeForFilter(),
            BigInteger.valueOf(range.last).encodeForFilter()
        )
        logger.info(marker, "loading logs $finalFilter range=$range")

        return ethereum.ethGetLogsJava(finalFilter)
            .doOnNext {
                logger.info(marker, "loaded ${it.size} logs for range $range")
            }.flatMapIterable { allLogs ->
                allLogs.groupBy { log ->
                    log.blockHash()
                }.entries.map { e ->
                    val orderedLogs = orderByTransaction(e.value)
                    BlockLogs(e.key.toString(), orderedLogs)
                }
            }
    }

    private fun orderByTransaction(logs: List<Log>): List<Log> {
        return logs.groupBy {
            it.transactionHash()
        }.values.flatMap { logsInTransaction ->
            logsInTransaction.sortedBy { log ->
                log.logIndex()
            }
        }
    }
}

private fun BigInteger.encodeForFilter(): String {
    return "0x${Hex.to(this.toByteArray()).trimStart('0')}"
}
