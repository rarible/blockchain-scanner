package com.rarible.blockchain.scanner.ethereum.client.hyper

import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.client.AbstractRetryableClient
import com.rarible.blockchain.scanner.client.NonRetryableBlockchainClientException
import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.model.ReceivedBlock
import com.rarible.blockchain.scanner.util.BlockRanges
import com.rarible.core.common.asyncWithTraceId
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.jdk.javaapi.CollectionConverters
import scalether.domain.response.Block
import scalether.domain.response.Log
import scalether.domain.response.Transaction
import java.math.BigInteger

@ExperimentalCoroutinesApi
class HyperArchiveEthereumClient(
    private val hyperBlockArchiverAdapter: HyperBlockArchiverAdapter,
    private val blockRepository: BlockRepository,
    private val properties: EthereumScannerProperties,
) : EthereumBlockchainClient, AbstractRetryableClient(ClientRetryPolicyProperties(attempts = 0)) {

    init {
        logger.info("Creating HyperArchiveEthereumClient")
    }

    override suspend fun getLastBlockNumber(): Long {
        return blockRepository.getLastBlock()?.id ?: properties.scan.firstAvailableBlock
    }

    override val newBlocks: Flow<EthereumBlockchainBlock> = emptyFlow()

    override suspend fun getBlocks(numbers: List<Long>): List<EthereumBlockchainBlock> =
        coroutineScope { numbers.map { asyncWithTraceId(context = NonCancellable) { getBlock(it) } }.awaitAll() }

    override suspend fun getBlock(number: Long): EthereumBlockchainBlock {
        val block = hyperBlockArchiverAdapter.getBlock(BigInteger.valueOf(number))
        return EthereumBlockchainBlock(ReceivedBlock(block))
    }

    override suspend fun getFirstAvailableBlock(): EthereumBlockchainBlock {
        return getBlock(properties.scan.firstAvailableBlock)
    }

    override fun getBlockLogs(
        descriptor: EthereumDescriptor,
        blocks: List<EthereumBlockchainBlock>,
        stable: Boolean
    ): Flow<FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>> {
        require(stable) { "HyperArchiveEthereumClient supports only stable blocks" }
        val ranges = BlockRanges.toRanges(blocks.map { it.number }).asFlow()
        return ranges.map { getStableLogs(descriptor, blocks, it) }.flattenConcat()
    }

    private fun getStableLogs(
        descriptor: EthereumDescriptor,
        blocks: List<EthereumBlockchainBlock>,
        range: LongRange
    ): Flow<FullBlock<EthereumBlockchainBlock, EthereumBlockchainLog>> = flow {
        val allLogs = coroutineScope {
            range.chunked(range.count())
                .map { LongRange(it.first(), it.last()) }
                .map {
                    asyncWithTraceId(context = NonCancellable) {
                        getLogsByRange(descriptor, it)
                    }
                }
                .awaitAll()
                .flatten()
        }

        val blocksMap = blocks.map { it.ethBlock }.associateBy { it.number() }
        coroutineScope {
            allLogs.groupBy { log ->
                log.blockNumber()
            }.entries.map { (blockNumber, blockLogs) ->
                asyncWithTraceId(context = NonCancellable) {
                    val ethFullBlock = blocksMap.getOrElse(blockNumber) {
                        hyperBlockArchiverAdapter.getBlock(blockNumber)
                    }
                    createFullBlock(ethFullBlock, blockLogs)
                }
            }.awaitAll()
        }.forEach { emit(it) }
    }

    private suspend fun getLogsByRange(descriptor: EthereumDescriptor, range: LongRange): List<Log> {
        val fromBlock = BigInteger.valueOf(range.first)
        val toBlock = BigInteger.valueOf(range.last)
        val logs = hyperBlockArchiverAdapter.getLogsByBlockRange(fromBlock, toBlock)

        return logs.filter { log ->
            val matchesToTopic = log.topics().head().equals(descriptor.ethTopic)
            val matchesToContracts = descriptor.contracts.isEmpty() || descriptor.contracts.contains(log.address())
            matchesToTopic && matchesToContracts && !ignoreLog(log)
        }
    }

    private fun ignoreLog(log: Log): Boolean {
        return log.removed()
    }

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
        val transactions = CollectionConverters
            .asJava(ethFullBlock.transactions())
            .associateBy { it.hash() }

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
        val logger: Logger = LoggerFactory.getLogger(HyperArchiveEthereumClient::class.java)
    }
}
