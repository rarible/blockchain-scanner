package com.rarible.blockchain.scanner.reindex

import com.rarible.blockchain.scanner.block.Block
import com.rarible.blockchain.scanner.block.BlockService
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.blockchain.scanner.framework.model.LogStorage
import com.rarible.blockchain.scanner.handler.TypedBlockRange
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.lastOrNull
import org.slf4j.LoggerFactory

class BlockChecker<
    BB : BlockchainBlock,
    BL : BlockchainLog,
    R : LogRecord,
    D : Descriptor<S>,
    S : LogStorage
    >(
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val blockService: BlockService,
    private val reindexer: BlockReindexer<BB, BL, R, D, S>,
    private val planner: BlockScanPlanner<BB>,
    private val properties: BlockchainScannerProperties
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun check(blocksRanges: Flow<TypedBlockRange>): Flow<Block> {
        return flow {
            blocksRanges.collect { checkRange ->
                for (blockNumber in checkRange.range) {
                    val checkedBlock = checkBlock(blockNumber)
                    emit(checkedBlock)
                }
            }
        }
    }

    private suspend fun checkBlock(blockNumber: Long): Block {

        val blockchainBlock = blockchainClient.getBlock(blockNumber)
            ?: throw IllegalStateException("Can't get stable block $blockNumber from blockchain")

        val checkBlock = blockService.getBlock(blockNumber)
            ?: throw IllegalStateException("Can't get indexed block $blockNumber")

        if (blockchainBlock.hash != checkBlock.hash) {
            logger.info(
                "Found invalid block $blockNumber: " +
                    "indexed block hash ${checkBlock.hash}," +
                    " expected blockchain hash ${blockchainBlock.hash}"
            )
            val blockRange = BlockRange(
                from = blockNumber,
                to = blockNumber,
                batchSize = properties.scan.batchLoad.batchSize
            )
            logger.info("Start reindex invalid blocks: range=${blockRange.from}..${blockRange.to}")
            val plan = planner.getPlan(blockRange, state = null)
            reindexer.reindex(plan.baseBlock, plan.ranges).lastOrNull()
        }

        return checkBlock
    }
}
