package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.hedera.client.rest.HederaRestApiClient
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaBlockRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaOrder
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampFrom
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTimestampTo
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionRequest
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransactionType
import com.rarible.blockchain.scanner.hedera.model.HederaDescriptor
import com.rarible.blockchain.scanner.hedera.model.HederaLog
import com.rarible.blockchain.scanner.hedera.model.HederaTransactionFilter
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
                    val currentBlockNumber = getLastBlockNumber()
                    if (currentBlockNumber > latestBlockNumber.get()) {
                        val block = getBlock(currentBlockNumber)
                        if (block != null) {
                            latestBlockNumber.set(currentBlockNumber)
                            emit(block)
                        }
                    }
                } catch (e: Exception) {
                    logger.error("Error while getting new blocks", e)
                }
            }
        }

    override suspend fun getBlock(number: Long): HederaBlockchainBlock? {
        return try {
            val block = hederaRestApiClient.getBlockByHashOrNumber(number.toString())
            block.toBlockchainBlock()
        } catch (e: Exception) {
            logger.error("Failed to get block $number", e)
            null
        }
    }

    override suspend fun getBlocks(numbers: List<Long>): List<HederaBlockchainBlock> {
        return numbers.mapNotNull { getBlock(it) }
    }

    override suspend fun getFirstAvailableBlock(): HederaBlockchainBlock {
        val blocks = hederaRestApiClient.getBlocks(
            HederaBlockRequest(
                limit = 1,
                order = HederaOrder.ASC
            )
        ).blocks
        val block = blocks.firstOrNull() ?: throw IllegalStateException("No blocks available")
        return block.toBlockchainBlock()
    }

    override suspend fun getLastBlockNumber(): Long {
        val blocks = hederaRestApiClient.getBlocks(
            HederaBlockRequest(
                limit = 1,
                order = HederaOrder.DESC
            )
        ).blocks
        return blocks.firstOrNull()?.number ?: throw IllegalStateException("No blocks available")
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
                    transactionType = when (val filter = descriptor.filter) {
                        is HederaTransactionFilter.ByTransactionType -> HederaTransactionType.valueOf(filter.transactionType)
                        else -> null
                    }
                )
            ).transactions

            val logs = transactions.map { transaction ->
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
}
