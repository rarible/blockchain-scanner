package com.rarible.blockchain.scanner.reindex

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.block.BlockStatus
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.handler.TypedBlockRange
import com.rarible.blockchain.scanner.util.BlockRanges
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow

class BlockScanPlanner<BB : BlockchainBlock>(
    private val blockService: BlockService,
    private val blockchainClient: BlockchainBlockClient<BB>,
    properties: BlockchainScannerProperties
) {

    private val distance = properties.scan.batchLoad.confirmationBlockDistance
    private val batchSize = properties.scan.batchLoad.batchSize

    suspend fun getPlan(
        range: BlockRange,
        state: Long? = null,
    ): ScanPlan {
        val baseBlockId = state ?: maxOf(range.from - 1, 0)
        val baseBlock = getBlock(baseBlockId)
        val lastBlockNumber = blockchainClient.getLastBlockNumber()
        val from = baseBlock.id + 1
        val to = minOf(
            lastBlockNumber - distance,
            range.to ?: Long.MAX_VALUE
        )
        val blockRanges = BlockRanges.getRanges(
            from = from,
            to = to,
            step = range.batchSize ?: batchSize
        ).map { TypedBlockRange(it, true) }.asFlow()

        return ScanPlan(
            ranges = blockRanges,
            baseBlock = baseBlock,
            from = from,
            to = to
        )
    }

    private suspend fun getBlock(id: Long): Block {
        blockService.getBlock(id)?.let { return it }

        val block = blockchainClient.getBlock(id)
            ?: throw IllegalStateException("Block #$id can't be fetched")

        return Block(
            id = block.number,
            hash = block.hash,
            parentHash = block.parentHash,
            timestamp = block.timestamp,
            status = BlockStatus.SUCCESS,
            errors = emptyList()
        )
    }

    data class ScanPlan(
        val ranges: Flow<TypedBlockRange>,
        val baseBlock: Block,
        val from: Long,
        val to: Long,
    )
}
