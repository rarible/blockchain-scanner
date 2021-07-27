package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.data.BlockLogs
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor
import io.daonomic.rpc.domain.Word
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.RetryBackoffSpec
import scalether.core.EthPubSub
import scalether.core.MonoEthereum
import scalether.domain.Address
import scalether.domain.request.LogFilter
import scalether.domain.request.TopicFilter
import scalether.domain.response.Log
import scalether.util.Hex
import java.math.BigInteger
import java.util.*

class EthereumClient(
    private val ethereum: MonoEthereum,
    private val ethPubSub: EthPubSub,
    private val backoff: RetryBackoffSpec
) : BlockchainClient<EthereumBlockchainBlock, Log> {

    private val logger: Logger = LoggerFactory.getLogger(EthereumClient::class.java)

    override fun listenNewBlocks(): Flux<EthereumBlockchainBlock> {
        return ethPubSub.newHeads()
            .map { EthereumBlockchainBlock(it) }
    }

    override fun getBlock(hash: String): Mono<EthereumBlockchainBlock> {
        return ethereum.ethGetBlockByHash(Word.apply(hash)).map {
            EthereumBlockchainBlock(it)
        }
    }

    override fun getBlock(id: Long): Mono<EthereumBlockchainBlock> {
        return ethereum.ethGetBlockByNumber(BigInteger.valueOf(id)).map {
            EthereumBlockchainBlock(it)
        }
    }

    override fun getLastBlockNumber(): Mono<Long> {
        return ethereum.ethBlockNumber().map { it.toLong() }
    }

    override fun getTransactionMeta(transactionHash: String): Mono<Optional<TransactionMeta>> {
        return ethereum.ethGetTransactionByHash(Word.apply(transactionHash)).map {
            if (it.isEmpty) {
                Optional.empty()
            } else {
                val tx = it.get()
                Optional.of(
                    TransactionMeta(
                        tx.hash().toString(),
                        tx.blockNumber().toLong(),
                        tx.blockHash().toString()
                    )
                )
            }
        }
    }

    override fun getBlockEvents(
        block: EthereumBlockchainBlock,
        descriptor: LogEventDescriptor,
        marker: Marker
    ): Mono<List<Log>> {
        val filter = LogFilter
            .apply(TopicFilter.simple(Word.apply(descriptor.topic))) // TODO ???
            .address(*descriptor.contracts.map { Address.apply(it) }.toTypedArray())
            .blockHash(block.ethBlock.hash())

        return ethereum.ethGetLogsJava(filter)
            .map { orderByTransaction(it) }
            .doOnError { logger.warn(marker, "Unable to get logs for block ${block.ethBlock.hash()}", it) }
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
