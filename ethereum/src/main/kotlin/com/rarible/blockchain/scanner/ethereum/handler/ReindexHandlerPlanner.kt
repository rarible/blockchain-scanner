package com.rarible.blockchain.scanner.ethereum.handler

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.task.ReindexBlocksTaskHandler
import com.rarible.blockchain.scanner.handler.BlocksRange
import com.rarible.blockchain.scanner.util.BlockRanges
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.first
import org.springframework.stereotype.Component

@Component
class ReindexHandlerPlanner(
    private val blockService: BlockService,
    private val ethereumClient: EthereumBlockchainClient,
    private val blockchainScannerProperties: BlockchainScannerProperties
) {
    @Suppress("EXPERIMENTAL_API_USAGE")
    suspend fun getReindexPlan(
        taskParam: ReindexBlocksTaskHandler.TaskParam,
        from: Long?
    ): ReindexPlan {
        val baseBlockId = from ?: taskParam.range.from
        val baseBlock = blockService.getBlock(baseBlockId)
            ?: throw IllegalStateException("Block #$baseBlockId was never indexed")
        val lastBlock = ethereumClient.newBlocks.first()
        val blockRanges = BlockRanges.getStableUnstableBlockRanges(
            baseBlockNumber = baseBlock.id,
            lastBlockNumber = lastBlock.number,
            batchSize = blockchainScannerProperties.scan.batchLoad.batchSize,
            stableDistance = blockchainScannerProperties.scan.batchLoad.confirmationBlockDistance
        ).mapNotNull { blocksRange ->
            if (taskParam.range.to != null) {
                blocksRange.coerceWithMaxId(taskParam.range.to)
            } else {
                blocksRange
            }
        }.asFlow()
        return ReindexPlan(blockRanges, baseBlock)
    }

    private fun BlocksRange.coerceWithMaxId(maxId: Long): BlocksRange? {
        val (from, to) = range.first to range.last
        if (maxId < from) {
            return null
        }
        if (to < maxId) {
            return this
        }
        return copy(range = from..maxId)
    }

    data class ReindexPlan(
        val reindexRanges: Flow<BlocksRange>,
        val baseBlock: Block
    )
}
