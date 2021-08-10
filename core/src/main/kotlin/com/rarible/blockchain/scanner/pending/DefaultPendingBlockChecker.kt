package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.Source
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.service.BlockService
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

@FlowPreview
@ExperimentalCoroutinesApi
class DefaultPendingBlockChecker<BB : BlockchainBlock, BL : BlockchainLog, B : Block, D : Descriptor>(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val blockService: BlockService<B>,
    private val blockListener: BlockListener
) : PendingBlockChecker {

    private val logger = LoggerFactory.getLogger(DefaultPendingBlockChecker::class.java)

    override fun checkPendingBlocks(pendingBlockAgeToCheck: Duration) {
        runBlocking {
            logger.info("Starting to check pending blocks with min block age: {}", pendingBlockAgeToCheck)
            try {
                flowOf(
                    blockService.findByStatus(Block.Status.PENDING).filter { isOldEnough(it, pendingBlockAgeToCheck) },
                    blockService.findByStatus(Block.Status.ERROR)
                ).flattenConcat().map {
                    reindexPendingBlock(it)
                }.collect()
                logger.info("Finished checking pending blocks")
            } catch (e: Exception) {
                logger.error("Unexpected error during reindexing pending blocks:", e)
            }
        }
    }

    private fun isOldEnough(block: B, pendingBlockAgeToCheck: Duration): Boolean {
        val createdAt = Instant.ofEpochSecond(block.timestamp)
        val sinceCreated = Duration.between(createdAt, Instant.now())
        return !sinceCreated.minus(pendingBlockAgeToCheck).isNegative
    }

    private suspend fun reindexPendingBlock(block: B) {
        logger.info("Reindexing pending block: [{}]", block)
        val originalBlock = blockchainClient.getBlock(block.id)
        val event = BlockEvent(Source.PENDING, originalBlock)
        blockListener.onBlockEvent(event)
    }
}

