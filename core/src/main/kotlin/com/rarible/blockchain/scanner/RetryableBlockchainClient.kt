package com.rarible.blockchain.scanner

import com.github.michaelbull.retry.policy.constantDelay
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import com.rarible.blockchain.scanner.data.FullBlock
import com.rarible.blockchain.scanner.data.TransactionMeta
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import kotlinx.coroutines.flow.Flow
import org.slf4j.LoggerFactory

class RetryableBlockchainClient<BB : BlockchainBlock, BL : BlockchainLog, D : Descriptor>(
    private val original: BlockchainClient<BB, BL, D>,
    private val retryPolicy: ClientRetryPolicyProperties
) : BlockchainClient<BB, BL, D> {

    private val logger = LoggerFactory.getLogger(BlockchainClient::class.java)

    private val delay = retryPolicy.delay.toMillis()
    private val attempts = retryPolicy.attempts


    override fun listenNewBlocks(): Flow<BB> {
        return original.listenNewBlocks()
    }

    override suspend fun getBlock(number: Long): BB {
        return wrapWithRetry("getBlock", number) {
            original.getBlock(number)
        }
    }

    override suspend fun getBlock(hash: String): BB {
        return wrapWithRetry("getBlock", hash) {
            original.getBlock(hash)
        }
    }

    override suspend fun getLastBlockNumber(): Long {
        return wrapWithRetry("getLastBlockNumber") {
            original.getLastBlockNumber()
        }
    }

    override suspend fun getBlockEvents(descriptor: D, block: BB): Flow<BL> {
        return wrapWithRetry("getBlockEvents", block, descriptor) {
            original.getBlockEvents(descriptor, block)
        }
    }

    override suspend fun getBlockEvents(descriptor: D, range: LongRange): Flow<FullBlock<BB, BL>> {
        return wrapWithRetry("getBlockEvents", descriptor, range) {
            original.getBlockEvents(descriptor, range)
        }
    }

    override suspend fun getTransactionMeta(transactionHash: String): TransactionMeta? {
        return wrapWithRetry("getTransactionMeta", transactionHash) {
            original.getTransactionMeta(transactionHash)
        }
    }

    private suspend fun <T> wrapWithRetry(method: String, vararg args: Any, clientCall: suspend () -> T): T {
        try {
            return retry(limitAttempts(attempts) + constantDelay(delay)) {
                clientCall.invoke()
            }
        } catch (e: Throwable) {
            logger.error(
                "Unable to perform BlockchainClient operation '{}' with params [{}] after {} attempts: {}",
                method, listOf(args), retryPolicy.attempts, e.message
            )
            throw e
        }
    }
}
