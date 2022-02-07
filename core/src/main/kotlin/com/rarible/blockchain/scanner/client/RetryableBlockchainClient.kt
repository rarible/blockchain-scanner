package com.rarible.blockchain.scanner.client

import com.github.michaelbull.retry.policy.constantDelay
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.FullBlock
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

    override val newBlocks: Flow<BB> get() = original.newBlocks

    override suspend fun getFirstAvailableBlock(): BB {
        return wrapWithRetry("getBlock") {
            original.getFirstAvailableBlock()
        }
    }

    override suspend fun getBlock(number: Long): BB? {
        return wrapWithRetry("getBlock", number) {
            original.getBlock(number)
        }
    }

    override fun getBlockLogs(descriptor: D, blocks: List<BB>, stable: Boolean): Flow<FullBlock<BB, BL>> {
        return original.getBlockLogs(descriptor, blocks, stable)
            .wrapWithRetry()
    }

    private fun <T> Flow<T>.wrapWithRetry(): Flow<T> {
        return this
            .retry(retries = (attempts - 1).toLong()) {
                delay(delay)
                true
            }
    }

    protected suspend fun <T> wrapWithRetry(method: String, vararg args: Any, clientCall: suspend () -> T): T {
        try {
            return retry(limitAttempts(attempts) + constantDelay(delay)) {
                clientCall.invoke()
            }
        } catch (e: Throwable) {
            logger.error(
                "Unable to perform BlockchainClient operation '{}' with params [{}] after {} attempts: {}",
                method, args.contentToString(), retryPolicy.attempts, e.message
            )
            throw e
        }
    }
}
