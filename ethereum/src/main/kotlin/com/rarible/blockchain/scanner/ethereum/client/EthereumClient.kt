package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.framework.data.BlockHeader
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.util.BlockRanges
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import scala.jdk.javaapi.CollectionConverters
import scalether.core.EthPubSub
import scalether.core.MonoEthereum
import scalether.domain.Address
import scalether.domain.request.LogFilter
import scalether.domain.request.TopicFilter
import scalether.domain.response.Block
import scalether.domain.response.Log
import scalether.domain.response.Transaction
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

    override fun getBlockLogs(
        descriptor: EthereumDescriptor,
        blocks: List<BlockHeader>,
        stable: Boolean
    ): Flow<FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>> {
        return if (stable) {
            // Normally, we have only one consequent range here.
            val ranges = BlockRanges.toRanges(blocks.map { it.number }).asFlow()
            ranges.map { getStableLogs(descriptor, it) }.flattenConcat()
        } else {
            getUnstableBlockLogs(descriptor, blocks)
        }
    }

    private fun getStableLogs(
        descriptor: EthereumDescriptor,
        range: LongRange
    ) = flow {
        val addresses = descriptor.contracts.map { Address.apply(it) }.toTypedArray()
        val filter = LogFilter
            .apply(TopicFilter.simple(descriptor.ethTopic))
            .let { if (addresses.isNotEmpty()) it.address(*addresses) else it }
            .blocks(
                BigInteger.valueOf(range.first).encodeForFilter(),
                BigInteger.valueOf(range.last).encodeForFilter()
            )
        val allLogs = ethereum.ethGetLogsJava(filter).awaitFirst().filterNot { it.removed() }
        logger.info("Loaded ${allLogs.size} logs for topic ${descriptor.ethTopic} for blocks $range")
        allLogs.groupBy { log ->
            log.blockHash()
        }.entries.map { (blockHash, blockLogs) ->
            val ethFullBlock = ethereum.ethGetFullBlockByHash(blockHash).awaitFirst()
            createFullBlock(ethFullBlock, blockLogs)
        }.forEach { emit(it) }
    }

    private fun getUnstableBlockLogs(
        descriptor: EthereumDescriptor,
        blocks: List<BlockHeader>
    ): Flow<FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>> {
        val addresses = descriptor.contracts.map { Address.apply(it) }.toTypedArray()
        val filter = LogFilter
            .apply(TopicFilter.simple(descriptor.ethTopic))
            .let { if (addresses.isNotEmpty()) it.address(*addresses) else it }
        return blocks.asFlow().map { blockHeader ->
            val blockHash = Word.apply(blockHeader.hash)
            val finalFilter = filter.blockHash(blockHash)
            val allLogs = ethereum.ethGetLogsJava(finalFilter).awaitFirst().filterNot { it.removed() }
            logger.info("Loaded {} logs of topic {} for fresh block [{}:{}]", allLogs.size, descriptor.ethTopic, blockHeader.number, blockHash)
            val ethFullBlock = ethereum.ethGetFullBlockByHash(blockHash).awaitFirst()
            createFullBlock(ethFullBlock, allLogs)
        }
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

    private fun createFullBlock(
        ethFullBlock: Block<Transaction>,
        logsInBlock: List<Log>
    ): FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog> {
        val indexedEthLogs = attachIndex(logsInBlock)
        val transactions = CollectionConverters.asJava(ethFullBlock.transactions()).associateBy { it.hash() }
        return FullBlock(
            block = EthereumBlockchainBlock(ethFullBlock),
            logs = indexedEthLogs.map { (index, ethLog) ->
                val transaction = transactions[ethLog.transactionHash()]
                    ?: error(
                        "Transaction #${ethLog.transactionHash()} is not found in the block $ethFullBlock\n" +
                                "All transactions: $transactions"
                    )
                EthereumBlockchainLog(
                    ethLog = ethLog,
                    index = index,
                    ethTransaction = transaction
                )
            }
        )
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
