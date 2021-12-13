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

        val addresses = descriptor.contracts.map { Address.apply(it) }.toTypedArray()
        val filter = LogFilter
            .apply(TopicFilter.simple(descriptor.ethTopic))
            .let { if (addresses.isNotEmpty()) it.address(*addresses) else it }
            .blocks(
                BigInteger.valueOf(range.first).encodeForFilter(),
                BigInteger.valueOf(range.last).encodeForFilter()
            )
        logger.info("Loading logs with filter in range $range with [$filter]")

        return ethereum.ethGetLogsJava(filter)
            .flatMapIterable { allLogs ->
                logger.info("Loaded ${allLogs.size} logs for range $range")
                allLogs.groupBy { log ->
                    log.blockHash()
                }.entries.map { (blockHash, blockLogs) ->
                    val indexedLogs = attachIndex(blockLogs)
                    getBlock(blockHash).map { block ->
                        FullBlock(block, indexedLogs.map { EthereumBlockchainLog(it.value, it.index) })
                    }
                }
            }.concatMap { it }.asFlow()
    }

    /**
     * Attach [EthereumBlockchainLog.index] calculated by grouping <transactionHash, topic, address>
     * and sorting by <logIndex> in each group.
     * The topic is implicitly fixed here, so just group by <transactionHash, address>.
     */
    private fun attachIndex(logsInBlock: List<Log>): List<IndexedValue<Log>> {
        return logsInBlock.groupBy {
            it.transactionHash() to it.address()
        }.values.flatMap { group ->
            group.sortedBy { log ->
                log.logIndex()
            }.withIndex()
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
