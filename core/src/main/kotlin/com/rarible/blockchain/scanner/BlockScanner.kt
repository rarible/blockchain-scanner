package com.rarible.blockchain.scanner

import com.github.michaelbull.retry.ContinueRetrying
import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.constantDelay
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.rarible.blockchain.scanner.configuration.ScanRetryPolicyProperties
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.model.BlockMeta
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.metrics.Metrics
import com.rarible.blockchain.scanner.util.flatten
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory

@FlowPreview
@ExperimentalCoroutinesApi
class BlockScanner<BB : BlockchainBlock, BL : BlockchainLog, B : Block, D : Descriptor>(
    metrics: Metrics,
    private val blockchainClient: BlockchainClient<BB, BL, D>,
    private val blockMapper: BlockMapper<BB, B>,
    private val blockService: BlockService<B>,
    private val retryPolicy: ScanRetryPolicyProperties,
    // This buffer will be filled only with deferred objects, so memory consumption should be insignificant
    private val blockBufferSize: Int
) {

    private val logger = LoggerFactory.getLogger(BlockScanner::class.java)

    private val blockTimer = metrics.timed("scanner.block.process")
    private val blockSaveTimer = metrics.timed("scanner.block.save")

    private val delay = retryPolicy.reconnectDelay.toMillis()
    private val attempts = if (retryPolicy.reconnectAttempts > 0) {
        retryPolicy.reconnectAttempts
    } else {
        Integer.MAX_VALUE
    }

    suspend fun scan(blockListener: BlockListener) {
        val retryOnFlowCompleted: RetryPolicy<Throwable> = {
            logger.warn("Blockchain scanning interrupted with cause:", reason)
            logger.info("Will try to reconnect to blockchain in ${retryPolicy.reconnectDelay}")
            ContinueRetrying
        }

        retry(retryOnFlowCompleted + limitAttempts(attempts) + constantDelay(delay)) {
            logger.info("Connecting to blockchain...")
            val blockFlow = getEventFlow()
            logger.info("Connected to blockchain, starting to receive events")
            blockFlow.map {
                blockTimer.record {
                    blockListener.onBlockEvent(it)
                }
            }.collect()
            throw IllegalStateException("Disconnected from Blockchain, event flow completed")
        }
    }

    private fun getEventFlow(): Flow<BlockEvent> = flatten {
        blockchainClient.listenNewBlocks().flatMapConcat { newBlock ->
            blockSaveTimer.record {
                getNewBlocks(newBlock).flatMapConcat { saveBlock(it) }
            }
        }
    }

    private fun getNewBlocks(newBlock: BB): Flow<BB> = flatten {
        logger.info("Checking for not-indexed blocks previous to new on with number: {}", newBlock.number)

        val lastKnown = try {
            blockService.getLastBlock()
        } catch (e: Exception) {
            logger.warn(e.message, e)
            null
        }
        if (lastKnown == null || lastKnown.id == newBlock.number) {
            logger.info("Last indexed block is {}, handling only new block: [{}]", lastKnown, newBlock.meta)
            flowOf(newBlock)
        } else {
            val range = (lastKnown.id + 1) until newBlock.number
            logger.info("Found last known block with number: {}, processing block range: {}", lastKnown.id, range)
            val asyncBlockList = range.asFlow().map { getBlockAsync(it) }.buffer(blockBufferSize).map { it.await() }
            flowOf(asyncBlockList, flowOf(newBlock)).flattenConcat()
        }
    }

    private suspend fun getBlockAsync(number: Long) = coroutineScope {
        async { blockchainClient.getBlock(number) }
    }

    /**
     * when inserting/updating block we need to inspect parent blocks if chain was reorganized
     */
    private fun saveBlock(newBlock: BB, depth: Int = 0): Flow<BlockEvent> = flatten {
        logger.info("Saving block: [{}]", newBlock)
        val parentBlock = blockService.getBlock(newBlock.number - 1)
        val parentBlockFlow = when {
            //do nothing if parent hash not found (just started listening to blocks)
            parentBlock == null -> {
                logger.info(
                    "There is no indexed parent for Block [{}], stopping to retrieve chain of changes",
                    newBlock.meta
                )
                emptyFlow()
            }

            //do nothing if parent hash is the same
            parentBlock.hash == newBlock.parentHash -> {
                logger.info(
                    "Parent is the same for new Block and indexed Block [{}] -> '{}', " +
                        "stopping to retrieve chain of changes", newBlock.meta, parentBlock
                )
                emptyFlow()
            }

            //fetch parent block and save it if parent block hash changed
            else -> {
                val internalParentBlock = blockchainClient.getBlock(newBlock.number - 1)
                logger.info(
                    "Going to save parent Block [{}], current chain depth is {}",
                    internalParentBlock.meta, depth
                )
                saveBlock(internalParentBlock, depth + 1)
            }
        }

        flowOf(parentBlockFlow, saveBlockIfChanged(newBlock)).flattenConcat()
    }

    private fun saveBlockIfChanged(block: BB): Flow<BlockEvent> = flatten {
        val exist = blockService.getBlock(block.number)

        logger.info("Checking new Block: [{}]", block.meta)
        when {
            exist == null -> {
                logger.info("Block with number and {} hash '{}' NOT found, this is new block", block.number, block.hash)
                blockService.save(blockMapper.map(block))
                flowOf(BlockEvent(Source.BLOCKCHAIN, block))
            }
            exist.hash != block.hash -> {
                logger.info(
                    "Block with number and {} hash '{}' found, but hash is different: {}",
                    block.number, block.hash, exist
                )
                blockService.save(blockMapper.map(block))
                val revertedBlock = BlockMeta(exist.id, exist.hash, exist.parentHash, exist.timestamp)
                flowOf(BlockEvent(Source.BLOCKCHAIN, block.meta, revertedBlock))
            }
            else -> {
                logger.info(
                    "Block with number and {} hash '{}' found, hash is the same: {}",
                    block.number, block.hash, exist
                )
                emptyFlow()
            }
        }.onCompletion {
            logger.info("Checking of new Block completed: [{}]", block.meta)
        }
    }
}
