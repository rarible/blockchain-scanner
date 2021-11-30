package com.rarible.blockchain.scanner.event.block

import com.github.michaelbull.retry.ContinueRetrying
import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.constantDelay
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import kotlinx.coroutines.flow.collect
import org.slf4j.LoggerFactory

class BlockScanner<BB : BlockchainBlock, B : Block>(
    private val blockMapper: BlockMapper<BB, B>,
    private val blockClient: BlockchainBlockClient<BB>,
    private val blockService: BlockService<B>,
    private val retryPolicy: ScanRetryPolicyProperties
) {

    private val logger = LoggerFactory.getLogger(BlockScanner::class.java)

    private val delay = retryPolicy.reconnectDelay.toMillis()
    private val attempts = if (retryPolicy.reconnectAttempts > 0) {
        retryPolicy.reconnectAttempts
    } else {
        Integer.MAX_VALUE
    }

    suspend fun scan(blockEventPublisher: BlockEventPublisher) {
        val retryOnFlowCompleted: RetryPolicy<Throwable> = {
            logger.warn("Blockchain scanning interrupted with cause:", reason)
            logger.info("Will try to reconnect to blockchain in ${retryPolicy.reconnectDelay}")
            ContinueRetrying
        }

        val handler = BlockHandler(
            blockMapper,
            blockClient,
            blockService,
            blockEventPublisher
        )

        retry(retryOnFlowCompleted + limitAttempts(attempts) + constantDelay(delay)) {
            logger.info("Connecting to blockchain...")
            blockClient.newBlocks.collect { handler.onNewBlock(it) }
            throw IllegalStateException("Disconnected from Blockchain, event flow completed")
        }
    }
}
