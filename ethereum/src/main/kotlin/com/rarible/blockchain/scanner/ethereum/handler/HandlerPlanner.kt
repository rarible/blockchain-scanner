package com.rarible.blockchain.scanner.ethereum.handler

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.block.BlockStatus
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainClient
import com.rarible.blockchain.scanner.ethereum.model.BlockRange
import com.rarible.blockchain.scanner.handler.BlocksRange
import com.rarible.blockchain.scanner.util.BlockRanges
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import org.springframework.stereotype.Component

@Component
class HandlerPlanner(
    private val blockService: BlockService,
    private val ethereumClient: EthereumBlockchainClient,
    private val blockchainScannerProperties: BlockchainScannerProperties
) {
    @Suppress("EXPERIMENTAL_API_USAGE")
    suspend fun getPlan(
        range: BlockRange,
        from: Long? = null,
    ): Plan {
        val baseBlockId = from ?: maxOf(range.from - 1, 0)
        val baseBlock = getBlock(baseBlockId)
        val lastBlockNumber = ethereumClient.getLatestBlockNumber()
        val blockRanges = BlockRanges.getRanges(
            from = baseBlock.id + 1,
            to = minOf(
                lastBlockNumber - blockchainScannerProperties.scan.batchLoad.confirmationBlockDistance,
                range.to ?: Long.MAX_VALUE
            ),
            step = range.batchSize ?: blockchainScannerProperties.scan.batchLoad.batchSize
        ).map { blocksRange ->
            BlocksRange(blocksRange, true)
        }.asFlow()

        return Plan(blockRanges, baseBlock)
    }

    private suspend fun getBlock(id: Long): Block {
        return blockService.getBlock(id)
            ?: run {
                val block = ethereumClient.getBlock(id)
                    ?: throw IllegalStateException("Block #$id can't be fetched")

                Block(
                    id = block.number,
                    hash = block.hash,
                    parentHash = block.parentHash,
                    timestamp = block.timestamp,
                    status = BlockStatus.SUCCESS
                )
            }
    }

    data class Plan(
        val ranges: Flow<BlocksRange>,
        val baseBlock: Block
    )
}
