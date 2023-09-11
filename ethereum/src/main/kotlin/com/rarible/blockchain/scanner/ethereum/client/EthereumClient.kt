package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.client.AbstractRetryableClient
import com.rarible.blockchain.scanner.client.NonRetryableBlockchainClientException
import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.core.common.asyncWithTraceId
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
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

@ExperimentalCoroutinesApi
@Component
class EthereumClient(
    ethereum: MonoEthereum,
    properties: EthereumScannerProperties,
    ethPubSub: EthPubSub,
    monitor: BlockchainMonitor,
    // Retries delegated to MonoEthereum
) : EthereumBlockchainClient, AbstractRetryableClient(ClientRetryPolicyProperties(attempts = 0)) {

    private val ethereum = EthereumRetryableMono(ethereum, properties.retryPolicy.client)

    private val maxBatches = properties.maxBatches.associate {
        val parts = it.split(":")
        Word.apply(parts[0]) to Integer.parseInt(parts[1])
    }

    init {
        logger.info("Creating EthereumClient with maxBatches: $maxBatches")
    }

    override suspend fun getLastBlockNumber(): Long {
        return ethereum.ethBlockNumber().awaitFirst().toLong()
    }

    private val subscriber = if (properties.blockPoller.enabled)
        EthereumNewBlockPoller(ethereum, properties.blockPoller.period) else EthereumNewBlockPubSub(ethPubSub)

    private val firstAvailableBlock = properties.scan.firstAvailableBlock

    override val newBlocks: Flow<EthereumBlockchainBlock> = subscriber.newHeads()
        .flatMap {
            logger.info("Detected new block from subscriber: ${it.blockNumber}")
            ethereum
                .ethGetFullBlockByHash(it.hash())
                .wrapWithRetry("ethGetFullBlockByHash", it.hash(), it.number())
        }
        .map { EthereumBlockchainBlock(it) }
        .timeout(Duration.ofMinutes(5))
        .doOnCancel {
            logger.info("Subscription canceled")
        }
        .doOnError { e ->
            logger.warn("Subscription error: ${e.message}", e)
        }
        .doOnComplete {
            logger.info("Subscription completed")
        }
        .asFlow()

    override suspend fun getBlocks(numbers: List<Long>): List<EthereumBlockchainBlock> =
        coroutineScope { numbers.map { asyncWithTraceId(context = NonCancellable) { getBlock(it) } }.awaitAll() }

    override suspend fun getBlock(number: Long): EthereumBlockchainBlock {
        return ethereum.ethGetFullBlockByNumber(BigInteger.valueOf(number)).map {
            EthereumBlockchainBlock(it)
        }.awaitFirst()
    }

    override suspend fun getFirstAvailableBlock(): EthereumBlockchainBlock = getBlock(firstAvailableBlock)

    override fun getBlockLogs(
        descriptor: EthereumDescriptor,
        blocks: List<EthereumBlockchainBlock>,
        stable: Boolean
    ): Flow<FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>> {
        return if (stable) {
            // Normally, we have only one consequent range here.
            val ranges = BlockRanges.toRanges(blocks.map { it.number }).asFlow()
            ranges.map { getStableLogs(descriptor, blocks, it) }.flattenConcat()
        } else {
            getUnstableBlockLogs(descriptor, blocks)
        }
    }

    private fun getStableLogs(
        descriptor: EthereumDescriptor,
        blocks: List<EthereumBlockchainBlock>,
        range: LongRange
    ) = flow {
        val allLogs = coroutineScope {
            val maxBatchSize = maxBatches[descriptor.ethTopic]
            range.chunked(maxBatchSize ?: range.count())
                .map { LongRange(it.first(), it.last()) }
                .map {
                    asyncWithTraceId(context = NonCancellable) {
                        getLogsByRange(descriptor, it)
                    }
                }
                .awaitAll()
                .flatten()
        }

        logger.info("Loaded ${allLogs.size} logs for topic ${descriptor.ethTopic} for blocks $range")

        val blocksMap = blocks.map { it.ethBlock }.associateBy { it.hash().toString() }
        coroutineScope {
            allLogs.groupBy { log ->
                log.blockHash()
            }.entries.map { (blockHash, blockLogs) ->
                asyncWithTraceId(context = NonCancellable) {
                    val ethFullBlock = blocksMap.getOrElse(blockHash.toString()) {
                        ethereum.ethGetFullBlockByHash(blockHash).awaitFirst()
                    }
                    createFullBlock(ethFullBlock, blockLogs)
                }
            }.awaitAll()
        }.forEach { emit(it) }
    }

    private suspend fun getLogsByRange(descriptor: EthereumDescriptor, range: LongRange): List<Log> {
        val addresses = descriptor.contracts.map { Address.apply(it) }.toTypedArray()
        val filter = LogFilter
            .apply(TopicFilter.simple(descriptor.ethTopic))
            .let { if (addresses.isNotEmpty()) it.address(*addresses) else it }
            .blocks(
                BigInteger.valueOf(range.first).encodeForFilter(),
                BigInteger.valueOf(range.last).encodeForFilter()
            )
        return ethereum.ethGetLogsJava(filter).awaitFirst().filterNot { it.removed() }
    }

    private fun getUnstableBlockLogs(
        descriptor: EthereumDescriptor,
        blocks: List<EthereumBlockchainBlock>
    ): Flow<FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>> {
        val addresses = descriptor.contracts.map { Address.apply(it) }.toTypedArray()
        val filter = LogFilter
            .apply(TopicFilter.simple(descriptor.ethTopic))
            .let { if (addresses.isNotEmpty()) it.address(*addresses) else it }
        return blocks.asFlow().map { blockHeader ->
            val blockHash = Word.apply(blockHeader.hash)
            val finalFilter = filter.blockHash(blockHash)
            val allLogs = ethereum.ethGetLogsJava(finalFilter).awaitFirst().filterNot { it.removed() }
            logger.info(
                "Loaded {} logs of topic {} for fresh block [{}:{}]",
                allLogs.size,
                descriptor.ethTopic,
                blockHeader.number,
                blockHash
            )
            val ethFullBlock = ethereum.ethGetFullBlockByHash(blockHash).awaitFirst()
            createFullBlock(ethFullBlock, allLogs)
        }
    }

    /**
     * Attach [EthereumBlockchainLog.index] calculated by grouping <transactionHash, topic, address>
     * and sorting by <logIndex> in each group.
     * The topic is implicitly fixed here, so just group by <transactionHash, address>.
     */
    private fun attachIndex(logsInBlock: List<Log>): List<Indexed<Log>> {
        return logsInBlock.groupBy {
            it.transactionHash() to it.address()
        }.values.flatMap { group ->
            group
                .sortedBy { log -> log.logIndex() }
                .mapIndexed { index, log -> Indexed(index, group.size, log) }
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
            logs = indexedEthLogs.map { (index, total, ethLog) ->
                val transaction = transactions[ethLog.transactionHash()]
                    ?: throw NonRetryableBlockchainClientException(
                        "Transaction #${ethLog.transactionHash()} is not found in the block $ethFullBlock\n" +
                            "All transactions: $transactions"
                    )
                EthereumBlockchainLog(
                    ethLog = ethLog,
                    ethTransaction = transaction,
                    index = index,
                    total = total,
                )
            }
        )
    }

    private data class Indexed<out T>(val index: Int, val total: Int, val value: T)

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(EthereumClient::class.java)
    }
}

private fun BigInteger.encodeForFilter(): String {
    return if (this == BigInteger.ZERO) "0x0" else "0x${Hex.to(this.toByteArray()).trimStart('0')}"
}
