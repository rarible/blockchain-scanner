package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.job.PendingBlocksCheckJob
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.math.abs

@ExperimentalCoroutinesApi
class DefaultPendingBlockChecker<BB : BlockchainBlock, BL : BlockchainLog, B : Block>(
    private val blockchainClient: BlockchainClient<BB, BL>,
    private val blockService: BlockService<B>,
    private val blockListener: BlockListener
) : PendingBlockChecker {

    override fun checkPendingBlocks() {
        runBlocking {
            logger.info("started")
            try {
                merge(
                    blockService.findByStatus(Block.Status.PENDING).filter {
                        abs(System.currentTimeMillis() / 1000 - it.timestamp) > 60
                    },
                    blockService.findByStatus(Block.Status.ERROR)
                ).map {
                    reindexPendingBlock(it)
                }.collect()
                logger.info("ended")
            } catch (e: Exception) {
                logger.error("error", e)
            }
        }
    }

    suspend fun reindexPendingBlock(block: B) {
        logger.info("reindexing block {}", block)
        val block = blockchainClient.getBlock(block.id)
        val event = BlockEvent(block)
        blockListener.onBlockEvent(event)
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PendingBlocksCheckJob::class.java)
    }
}

