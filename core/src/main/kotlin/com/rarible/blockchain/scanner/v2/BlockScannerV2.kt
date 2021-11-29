package com.rarible.blockchain.scanner.v2

import com.github.michaelbull.retry.ContinueRetrying
import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.constantDelay
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.v2.event.BlockEvent
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.slf4j.LoggerFactory

class BlockScannerV2<BB : BlockchainBlock, B : Block>(
    private val blockMapper: BlockMapper<BB, B>,
    private val blockClient: BlockchainBlockClient<BB>,
    private val blockService: BlockService<B>,
    private val retryPolicy: ScanRetryPolicyProperties
) {

    private val logger = LoggerFactory.getLogger(BlockListener::class.java)

    private val delay = retryPolicy.reconnectDelay.toMillis()
    private val attempts = if (retryPolicy.reconnectAttempts > 0) {
        retryPolicy.reconnectAttempts
    } else {
        Integer.MAX_VALUE
    }

    suspend fun scan() {
        val retryOnFlowCompleted: RetryPolicy<Throwable> = {
            logger.warn("Blockchain scanning interrupted with cause:", reason)
            logger.info("Will try to reconnect to blockchain in ${retryPolicy.reconnectDelay}")
            ContinueRetrying
        }

        retry(retryOnFlowCompleted + limitAttempts(attempts) + constantDelay(delay)) {
            logger.info("Connecting to blockchain...")
            blockEvents.collect()
            throw IllegalStateException("Disconnected from Blockchain, event flow completed")
        }
    }

    val blockEvents: Flow<BlockEvent> = flow {
        val handler = BlockHandlerV2(
            blockMapper,
            blockClient,
            blockService
        ) { this.emit(it) }

        blockClient.newBlocks.collect { handler.onNewBlock(it) }
    }
}
