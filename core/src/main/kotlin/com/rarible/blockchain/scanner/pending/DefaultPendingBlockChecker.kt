package com.rarible.blockchain.scanner.pending

import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.job.PendingBlocksCheckJob
import com.rarible.core.logging.LoggingUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.Marker
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import kotlin.math.abs

class DefaultPendingBlockChecker<OB : BlockchainBlock, OL : BlockchainLog, B : Block>(
    private val blockchainClient: BlockchainClient<OB, OL>,
    private val blockService: BlockService<B>,
    private val blockListener: BlockListener
) : PendingBlockChecker {

    override fun checkPendingBlocks() {
        logger.info("started")
        try {
            Flux.concat(
                blockService.findByStatus(Block.Status.PENDING).filter {
                    abs(System.currentTimeMillis() / 1000 - it.timestamp) > 60
                },
                blockService.findByStatus(Block.Status.ERROR)
            )
                .concatMap { reindexPendingBlock(it) }
                .then().block()
            logger.info("ended")
        } catch (e: Exception) {
            logger.error("error", e)
        }
    }

    fun reindexPendingBlock(block: B): Mono<Void?> {
        return LoggingUtils.withMarker { marker: Marker? ->
            logger.info(marker, "reindexing block {}", block)
            blockchainClient.getBlock(block.id).flatMap {
                val event = BlockEvent(it)
                blockListener.onBlockEvent(event)
            }
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PendingBlocksCheckJob::class.java)
    }
}

