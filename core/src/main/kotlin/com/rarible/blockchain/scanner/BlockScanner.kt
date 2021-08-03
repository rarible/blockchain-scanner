package com.rarible.blockchain.scanner

import com.github.michaelbull.retry.ContinueRetrying
import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.constantDelay
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.data.BlockMeta
import com.rarible.blockchain.scanner.data.Source
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.util.flatten
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockScanner<BB : BlockchainBlock, BL : BlockchainLog, B : Block>(
    private val blockchainClient: BlockchainClient<BB, BL>,
    private val blockMapper: BlockMapper<BB, B>,
    private val blockService: BlockService<B>,
    private val properties: BlockchainScannerProperties
) {

    // TODO should be called in onApplicationStartedEvent in implementations
    suspend fun scan(blockListener: BlockListener) {
        val retryOnFlowCompleted: RetryPolicy<Throwable> = {
            logger.warn("Blockchain scanning interrupted with cause:", reason)
            logger.info("Will try to reconnect to blockchain in ${properties.reconnectDelay}")
            ContinueRetrying
        }
        retry(retryOnFlowCompleted + limitAttempts(Int.MAX_VALUE) + constantDelay(properties.reconnectDelay)) {
            logger.info("Connecting to blockchain...")
            val blockFlow = getEventFlow()
            logger.info("Connected to blockchain, starting to receive events")
            blockFlow.map {
                blockListener.onBlockEvent(it)
            }.onCompletion {
                throw IllegalStateException("Disconnected from Blockchain, event flow completed")
            }
        }
    }

    private fun getEventFlow(): Flow<BlockEvent> = flatten {
        blockchainClient.listenNewBlocks().flatMapConcat { newBlock ->
            getNewBlocks(newBlock).flatMapConcat { blockToUpdate ->
                saveBlock(blockToUpdate)
            }
        }
    }

    private fun getNewBlocks(newBlock: BB): Flow<BB> = flatten {
        logger.info("Checking for not-indexed blocks previous to new on with number: {}", newBlock.number)

        val lastKnown = blockService.getLastBlock()
        if (lastKnown == null) {
            logger.info("Last indexed block not found, will handle only new block: [{}]", newBlock.meta)
            flowOf(newBlock)
        } else {
            logger.info("Found last known block with number: {}", lastKnown)
            val range = (lastKnown + 1) until newBlock.number
            if (range.last >= range.first) {
                logger.info("Range of not-indexed blocks: {}", range)
            }
            val blockRangeFlow = range.asFlow().map { blockchainClient.getBlock(it) }
            merge(blockRangeFlow, flowOf(newBlock))
        }
    }

    /**
     * when inserting/updating block we need to inspect parent blocks if chain was reorganized
     */
    private fun saveBlock(newBlock: BB, depth: Int = 0): Flow<BlockEvent> = flatten {
        logger.info("Saving block: [{}]", newBlock)
        val parentBlockFlow = when (val parentBlockHash = blockService.getBlockHash(newBlock.number - 1)) {
            //do nothing if parent hash not found (just started listening to blocks)
            null -> {
                logger.info(
                    "There is no indexed parent for Block [{}], stopping to retrieve chain of changes",
                    newBlock.meta, parentBlockHash
                )
                emptyFlow()
            }

            //do nothing if parent hash is the same
            newBlock.parentHash -> {
                logger.info(
                    "Parent is the same for new Block and indexed Block [{}] -> '{}', " +
                            "stopping to retrieve chain of changes", newBlock.meta, parentBlockHash
                )
                emptyFlow()
            }

            //fetch parent block and save it if parent block hash changed
            else -> {
                val parentBlock = blockchainClient.getBlock(newBlock.number - 1)
                logger.info(
                    "Going to save parent Block [{}], current chain depth is {}",
                    parentBlock.meta, depth
                )
                saveBlock(parentBlock, depth + 1)
            }
        }

        merge(parentBlockFlow, checkNewBlock(newBlock))
    }

    private fun checkNewBlock(block: BB): Flow<BlockEvent> = flatten {
        val knownHash = blockService.getBlockHash(block.number)

        when {
            knownHash == null -> {
                logger.info("Block with number and {} hash '{}' NOT found, this is new block", block.number, block.hash)
                blockService.saveBlock(blockMapper.map(block))
                flowOf(BlockEvent(Source.BLOCKCHAIN, block))
            }
            knownHash != block.hash -> {
                logger.info(
                    "Block with number and {} hash '{}' found, but hash is different: {}",
                    block.number, block.hash, knownHash
                )
                blockService.saveBlock(blockMapper.map(block))
                val revertedBlock = BlockMeta(block.number, knownHash, block.parentHash, block.timestamp)
                flowOf(BlockEvent(Source.BLOCKCHAIN, block.meta, revertedBlock))
            }
            else -> {
                logger.info(
                    "Block with number and {} hash '{}' found, hash is the same: {}",
                    block.number, block.hash, knownHash
                )
                emptyFlow()
            }
        }.onEach {
            logger.info("Checking new Block: [{}]", block.meta)
        }.onCompletion {
            logger.info("Checking of new Block completed: [{}]", block.meta)
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(BlockScanner::class.java)
    }
}