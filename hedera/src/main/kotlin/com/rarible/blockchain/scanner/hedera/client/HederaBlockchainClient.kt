package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.hedera.client.rest.HederaRestApiClient
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlock
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaOrder
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampFrom
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampTo
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionRequest
import com.rarible.blockchain.scanner.hedera.model.HederaDescriptor
import com.rarible.blockchain.scanner.hedera.model.HederaLog
import com.rarible.core.common.asyncWithTraceId
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

@Component
class HederaBlockchainClient(
    private val hederaRestApiClient: HederaRestApiClient
) : BlockchainClient<HederaBlockchainBlock, HederaBlockchainLog, HederaDescriptor> {

    private val logger = LoggerFactory.getLogger(HederaBlockchainClient::class.java)

    override val newBlocks: Flow<HederaBlockchainBlock>
        get() = flow {
            val latestBlockNumber = AtomicLong(-1)

            while (true) {
                try {
                    val block = getLatestBlock()
                    if (block != null && block.number > latestBlockNumber.get()) {
                        latestBlockNumber.set(block.number)
                        emit(block.toBlockchainBlock())
                    }
                } catch (e: Exception) {
                    logger.error("Error while getting new blocks", e)
                }
            }
        }

    override suspend fun getBlock(number: Long): HederaBlockchainBlock? {
        val block = hederaRestApiClient.getBlockByHashOrNumber(number.toString())
        return block.toBlockchainBlock()
    }

    override suspend fun getBlocks(numbers: List<Long>) = coroutineScope {
        numbers.map { number -> asyncWithTraceId { getBlock(number) } }.awaitAll().filterNotNull()
    }

    override suspend fun getFirstAvailableBlock(): HederaBlockchainBlock {
        val blocks = hederaRestApiClient.getBlocks(EARLIEST_BLOCK_REQUEST).blocks
        val block = blocks.firstOrNull() ?: throw IllegalStateException("No blocks available")
        return block.toBlockchainBlock()
    }

    override suspend fun getLastBlockNumber(): Long {
        return getLatestBlock()?.number ?: throw IllegalStateException("No blocks available")
    }

    override fun getBlockLogs(
        descriptor: HederaDescriptor,
        blocks: List<HederaBlockchainBlock>,
        stable: Boolean
    ): Flow<FullBlock<HederaBlockchainBlock, HederaBlockchainLog>> = flow {
        for (block in blocks) {
            val transactions = hederaRestApiClient.getTransactions(
                HederaTransactionRequest(
                    timestampFrom = HederaTimestampFrom.Gte(block.consensusTimestampFrom),
                    timestampTo = HederaTimestampTo.Lte(block.consensusTimestampTo),
                )
            ).transactions

            val logs = transactions.filter { descriptor.filter.matches(it) }.map { transaction ->
                HederaBlockchainLog(
                    log = HederaLog(
                        blockNumber = block.number,
                        blockHash = block.hash,
                        consensusTimestamp = transaction.consensusTimestamp,
                        transactionHash = transaction.transactionHash,
                        transactionId = transaction.transactionId
                    ),
                    transaction = transaction
                )
            }
            emit(FullBlock(block, logs))
        }
    }

    private suspend fun getLatestBlock(): HederaBlock? {
        val blocks = hederaRestApiClient.getBlocks(LATEST_BLOCK_REQUEST).blocks
        return blocks.firstOrNull()
    }

    private companion object {
        val LATEST_BLOCK_REQUEST = HederaBlockRequest(
            limit = 1,
            order = HederaOrder.DESC
        )

        val EARLIEST_BLOCK_REQUEST = HederaBlockRequest(
            limit = 1,
            order = HederaOrder.ASC
        )
    }
}
