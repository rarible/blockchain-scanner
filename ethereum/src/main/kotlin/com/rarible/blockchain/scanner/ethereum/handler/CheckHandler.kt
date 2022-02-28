package com.rarible.blockchain.scanner.ethereum.handler

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.client.RetryableBlockchainClient
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.BlockRange
import com.rarible.blockchain.scanner.handler.BlocksRange
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@ExperimentalCoroutinesApi
@Component
class CheckHandler(
    ethereumClient: EthereumBlockchainClient,
    private val blockService: BlockService,
    private val reindexHandler: ReindexHandler,
    private val handlerPlanner: HandlerPlanner,
    private val blockchainScannerProperties: EthereumScannerProperties
) {
    private val retryableClient = RetryableBlockchainClient(
        original = ethereumClient,
        retryPolicy = blockchainScannerProperties.retryPolicy.client
    )

    suspend fun check(blocksRanges: Flow<BlocksRange>): Flow<Block> {
        return flow {
            blocksRanges.collect { checkRange ->
                val invalidBlocks = mutableListOf<Block>()
                for (blockNumber in checkRange.range) {
                    val blockchainBlock = retryableClient.getBlock(blockNumber)
                        ?: throw IllegalStateException("Can't get stable block $blockNumber from blockchain")
                    val checkBlock = blockService.getBlock(blockNumber)
                        ?: throw IllegalStateException("Can't get indexed block $blockNumber")

                    if (blockchainBlock.hash != checkBlock.hash) {
                        logger.info("Find invalid block $blockNumber: " +
                                "indexed block hash ${checkBlock.hash}," +
                                " expected blockchain hash ${blockchainBlock.hash}"
                        )
                        invalidBlocks.add(checkBlock)
                    } else {
                        if (invalidBlocks.isNotEmpty() && blockchainScannerProperties.task.checkBlocks.reindexBlocks) {
                            val blockRange = BlockRange(
                                from = invalidBlocks.first().id,
                                to = invalidBlocks.last().id
                            )
                            logger.info("Start reindex invalid blocks: range=${blockRange.from}..${blockRange.to}")
                            val plan = handlerPlanner.getPlan(blockRange, from = null)
                            reindexHandler.reindex(plan.baseBlock, plan.ranges)
                        }
                        invalidBlocks.clear()
                    }
                    emit(checkBlock)
                }
            }
        }
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(CheckHandler::class.java)
    }
}