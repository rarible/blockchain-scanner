package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import scalether.core.EthPubSub
import scalether.core.MonoEthereum
import scalether.domain.Address
import scalether.domain.request.LogFilter
import scalether.domain.request.TopicFilter
import scalether.domain.response.Log
import scalether.util.Hex
import java.math.BigInteger
import java.time.Duration

@Component
class EthereumClient(
    private val ethereum: MonoEthereum,
    private val ethPubSub: EthPubSub
) : BlockchainClient<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumDescriptor> {

    private val logger = LoggerFactory.getLogger(EthereumClient::class.java)

    override fun listenNewBlocks(): Flow<EthereumBlockchainBlock> {
        return ethPubSub.newHeads()
            .map { EthereumBlockchainBlock(it) }
            .timeout(Duration.ofMinutes(5))
            .asFlow()
    }

    override suspend fun getBlock(hash: String): EthereumBlockchainBlock {
        return getBlock(Word.apply(hash)).awaitFirst()
    }

    override suspend fun getBlock(number: Long): EthereumBlockchainBlock {
        return ethereum.ethGetBlockByNumber(BigInteger.valueOf(number)).map {
            EthereumBlockchainBlock(it)
        }.awaitFirst()
    }

    override suspend fun getLastBlockNumber(): Long {
        return ethereum.ethBlockNumber().map { it.toLong() }.awaitFirst()
    }

    override suspend fun getTransactionMeta(transactionHash: String): TransactionMeta? {
        val opt = ethereum.ethGetTransactionByHash(Word.apply(transactionHash)).awaitFirst()
        return if (opt.isEmpty) {
            null
        } else {
            val tx = opt.get()
            TransactionMeta(
                hash = tx.hash().toString(),
                blockHash = tx.blockHash().toString()
            )
        }
    }

    override fun getBlockEvents(
        descriptor: EthereumDescriptor,
        block: EthereumBlockchainBlock
    ): Flow<EthereumBlockchainLog> {
        val filter = LogFilter
            .apply(TopicFilter.simple(descriptor.topic))
            .address(*descriptor.contracts.toTypedArray())
            .blockHash(block.ethBlock.hash())

        return ethereum.ethGetLogsJava(filter)
            .map { orderByTransaction(it).map { log -> EthereumBlockchainLog(log) } }
            .flatMapMany { it.toFlux() }
            .asFlow()
    }

    //todo ??????????????, ???? ??????????????????, ?????? ?????????? ??????????????, ?????????? index ?????????????? ???????????? ?????????????? ???? ???????????? ?????????????????????
    //todo (???????????????????? ?????????? ???????????????? ???????????? ????????????????????, topic, address). ?????? ???? ???????? ???????
    override fun getBlockEvents(
        descriptor: EthereumDescriptor,
        range: LongRange
    ): Flow<FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>> {

        val addresses = descriptor.contracts.map { Address.apply(it) }
        val filter = LogFilter
            .apply(TopicFilter.simple(descriptor.topic))
            .address(*addresses.toTypedArray())
        val finalFilter = filter.blocks(
            BigInteger.valueOf(range.first).encodeForFilter(),
            BigInteger.valueOf(range.last).encodeForFilter()
        )
        logger.info("Loading logs with filter [$finalFilter] in range=$range")

        return ethereum.ethGetLogsJava(finalFilter)
            .flatMapIterable { allLogs ->
                logger.info("Loaded ${allLogs.size} logs for range $range")
                allLogs.groupBy { log ->
                    log.blockHash()
                }.entries.map { e ->
                    val orderedLogs = orderByTransaction(e.value)
                    val block = getBlock(e.key)
                    block.map { originalBlock ->
                        FullBlock(originalBlock, orderedLogs.map { EthereumBlockchainLog(it) })
                    }
                }
            }.concatMap { it }.asFlow()
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

    private fun getBlock(hash: Word): Mono<EthereumBlockchainBlock> {
        return ethereum.ethGetBlockByHash(hash).map {
            EthereumBlockchainBlock(it)
        }
    }
}

private fun BigInteger.encodeForFilter(): String {
    return "0x${Hex.to(this.toByteArray()).trimStart('0')}"
}
