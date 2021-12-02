package com.rarible.blockchain.scanner

import com.github.michaelbull.retry.policy.constantDelay
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import com.rarible.blockchain.scanner.framework.model.Descriptor
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.retry
import org.slf4j.LoggerFactory

class RetryableBlockchainClient<BB : BlockchainBlock, BL : BlockchainLog, D : Descriptor>(
    private val original: BlockchainClient<BB, BL, D>,
    private val retryPolicy: ClientRetryPolicyProperties
) : BlockchainClient<BB, BL, D> {

    private val logger = LoggerFactory.getLogger(BlockchainClient::class.java)

    private val delay = retryPolicy.delay.toMillis()
    private val attempts = retryPolicy.attempts

    override val newBlocks: Flow<BB> =
        original.newBlocks

    override suspend fun getBlock(number: Long): BB? {
        return wrapWithRetry("getBlock", number) {
            original.getBlock(number)
        }
    }

    override suspend fun getLastBlockNumber(): Long {
        return wrapWithRetry("getLastBlockNumber") {
            original.getLastBlockNumber()
        }
    }

    override fun getBlockEvents(descriptor: D, block: BB): Flow<BL> {
        return original.getBlockEvents(descriptor, block)
            .wrapWithRetry()
    }

    override fun getBlockEvents(descriptor: D, range: LongRange): Flow<FullBlock<BB, BL>> {
        return original.getBlockEvents(descriptor, range)
            .wrapWithRetry()
    }

    override suspend fun getTransactionMeta(transactionHash: String): TransactionMeta? {
        return wrapWithRetry("getTransactionMeta", transactionHash) {
            original.getTransactionMeta(transactionHash)
        }
    }

    private fun <T> Flow<T>.wrapWithRetry(): Flow<T> {
        return this
            .retry(retries = (attempts - 1).toLong()) {
                delay(delay)
                true
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
