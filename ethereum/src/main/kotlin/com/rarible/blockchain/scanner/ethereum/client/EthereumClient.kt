package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
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
    ethPubSub: EthPubSub
) : EthereumBlockchainClient {

    private val logger = LoggerFactory.getLogger(EthereumClient::class.java)

    override val newBlocks: Flow<EthereumBlockchainBlock> = ethPubSub.newHeads()
        .map { EthereumBlockchainBlock(it) }
        .timeout(Duration.ofMinutes(5))
        .asFlow()

    override suspend fun getBlock(number: Long): EthereumBlockchainBlock {
        return ethereum.ethGetBlockByNumber(BigInteger.valueOf(number)).map {
            EthereumBlockchainBlock(it)
        }.awaitFirst()
    }

    override suspend fun getBlock(hash: String): EthereumBlockchainBlock {
        return getBlock(Word.apply(hash)).awaitFirst()
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

    //todo помнишь, мы обсуждали, что нужно сделать, чтобы index события брался немного по другим параметрам?
    //todo (уникальный чтобы считался внутри транзакции, topic, address). это ты учел тут?
    override fun getBlockLogs(
        descriptor: EthereumDescriptor,
        range: LongRange
    ): Flow<FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>> {

        val addresses = descriptor.contracts.map { Address.apply(it) }
        val filter = LogFilter
            .apply(TopicFilter.simple(descriptor.ethTopic))
            .address(*addresses.toTypedArray())
            .blocks(
                BigInteger.valueOf(range.first).encodeForFilter(),
                BigInteger.valueOf(range.last).encodeForFilter()
            )
        logger.info("Loading logs with filter [$filter] in range $range")

        return ethereum.ethGetLogsJava(filter)
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
    return if (this == BigInteger.ZERO) "0x0" else "0x${Hex.to(this.toByteArray()).trimStart('0')}"
}
