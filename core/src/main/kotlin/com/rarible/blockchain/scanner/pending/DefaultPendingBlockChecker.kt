package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.event.block.BlockListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.service.BlockService
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
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

    // TODO make it in batches
    override suspend fun checkPendingBlocks(pendingBlockAgeToCheck: Duration) {
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

    private fun isOldEnough(block: B, pendingBlockAgeToCheck: Duration): Boolean {
        val createdAt = block.timestamp()
        val sinceCreated = Duration.between(createdAt, Instant.now())
        return !sinceCreated.minus(pendingBlockAgeToCheck).isNegative
    }

    private suspend fun reindexPendingBlock(block: B) {
        logger.info("Reindexing pending block: [{}:{}]", block.id, block.hash)
        val originalBlock = blockchainClient.getBlock(block.id)!! //todo check this change
        val event = NewBlockEvent(Source.PENDING, originalBlock.number, originalBlock.hash)
        blockListener.onBlockEvents(listOf(event))
    }
}

