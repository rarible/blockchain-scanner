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
import kotlin.coroutines.cancellation.CancellationException

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

    override suspend fun getLastBlockNumber(): Long {
        return wrapWithRetry("getLatestBlockNumber") {
            original.getLastBlockNumber()
        }
    }

    override suspend fun getBlocks(numbers: List<Long>): List<BB> {
        return wrapWithRetry("getBlocks", numbers) {
            original.getBlocks(numbers)
        }
    }

    override suspend fun getBlock(number: Long): BB? {
        return wrapWithRetry("getBlock", number) {
            original.getBlock(number)
        }
    }

    override fun getBlockLogs(descriptor: D, blocks: List<BB>, stable: Boolean): Flow<FullBlock<BB, BL>> {
        return original.getBlockLogs(descriptor, blocks, stable)
            .wrapWithRetry("getBlockLogs")
    }

    private fun <T> Flow<T>.wrapWithRetry(method: String = ""): Flow<T> {
        logger.info("WrapWithRetry: $method") //TODO: Remove
        return this
            .retry(retries = (attempts - 1).toLong()) {
                logger.info("WrapWithRetry retry: $method") //TODO: Remove
                delay(delay)
                true
            }
    }

    private suspend fun <T> wrapWithRetry(method: String, vararg args: Any, clientCall: suspend () -> T): T {
        try {
            logger.info("WrapWithRetry: $method") //TODO: Remove
            return retry(limitAttempts(attempts) + constantDelay(delay)) {
                clientCall.invoke()
            }
        } catch (e: CancellationException) {
            logger.warn("WrapWithRetry CancellationException : $method") //TODO: Remove
            throw e
        } catch (e: Throwable) {
            logger.error(
                "Unable to perform BlockchainClient operation '{}' with params [{}] after {} attempts: {}",
                method, args.contentToString(), retryPolicy.attempts, e.message
            )
            throw e
        }
    }
}
