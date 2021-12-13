package com.rarible.blockchain.scanner.event.block

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.data.NewBlockEvent
import com.rarible.blockchain.scanner.framework.data.RevertedBlockEvent
import com.rarible.blockchain.scanner.framework.data.Source
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.publisher.BlockEventPublisher
import org.slf4j.LoggerFactory

class BlockHandler<BB : BlockchainBlock, B : Block>(//todo simplify generics, only one B is needed
    private val blockMapper: BlockMapper<BB, B>, //todo remove block mapper, not needed
    private val blockClient: BlockchainBlockClient<BB>,
    private val blockService: BlockService<B>,
    private val blockListener: BlockEventPublisher
) {

    private val logger = LoggerFactory.getLogger(BlockHandler::class.java)

    // We can cache lastKnown block in order to avoid huge amount of DB requests in
    // regular case of block processing
    @Volatile
    private var lastBlock: B? = null

    suspend fun onNewBlock(newBlockchainBlock: BB) {
        logger.info("Received new Block [{}:{}]", newBlockchainBlock.number, newBlockchainBlock.hash)
        val newBlock = blockMapper.map(newBlockchainBlock)

        val lastStateBlock = getLastKnownBlock()
        logger.info(
            "Last known Block [{}:{}], parentHash: {}",
            lastStateBlock.id, lastStateBlock.hash, lastStateBlock.parentHash
        )

        // If new block's parent hash is the same as hash of last known block, we could omit blockchain reorg check
        if (newBlockchainBlock.parentHash == lastStateBlock.hash) {
            updateBlock(newBlock)
            lastBlock = newBlock
            return
        }

        // Otherwise, we have missed blocks in our state OR chain was reorganized
        lastBlock = restoreMissedBlocks(lastStateBlock, newBlock)
    }

    private suspend fun restoreMissedBlocks(lastStateBlock: B, newBlock: B): B {
        var currentBlock = checkChainReorgAndRevert(lastStateBlock)

        while (currentBlock.id < newBlock.id) {
            val nextBlock = getNextBlock(currentBlock, newBlock.id - currentBlock.id)

            if (nextBlock.parentHash != currentBlock.hash) {
                // chain reorg occurred while we were going forward
                // stop this iteration. next time we will check reorg and go forward again
                logger.info(
                    "Chain has been reorganized during reading missed blocks: " +
                            "[{}:{}] is not a parent of [{}:{}], chain will be reorganized on next BlockEvent",
                    currentBlock.id, currentBlock.hash, newBlock.id, newBlock.hash
                )

                return currentBlock
            }

            updateBlock(nextBlock)
            currentBlock = nextBlock
        }
        return currentBlock
    }

    /**
     * Checks if chain reorg happened. Find the latest correct block and revert others in order
     * @param startStateBlock block from the state to start looking for reorg from
     * @return latest correct block found
     */
    private suspend fun checkChainReorgAndRevert(startStateBlock: B): B {
        var stateBlock = startStateBlock
        var blockchainBlock = fetchBlock(stateBlock.id)

        while (blockchainBlock == null || blockchainBlock.hash != stateBlock.hash) {
            notifyRevertedBlock(stateBlock)
            blockService.remove(stateBlock.id)

            stateBlock = getPreviousBlock(stateBlock)
            blockchainBlock = fetchBlock(stateBlock.id)
        }

        return stateBlock
    }

    private suspend fun getLastKnownBlock(): B {
        if (lastBlock != null) {
            return lastBlock!!
        }
        val lastKnownBlock = blockService.getLastBlock()
        if (lastKnownBlock != null) {
            lastBlock = lastKnownBlock
            return lastKnownBlock
        }

        logger.info("There is no last know Block, retrieving first one")
        val blockchainBlock = fetchBlock(0) ?: error("not found root block")

        logger.info("Found first Block in chain [{}:{}]", blockchainBlock.id, blockchainBlock.hash)
        updateBlock(blockchainBlock)

        lastBlock = blockchainBlock
        return blockchainBlock
    }

    private suspend fun getNextBlock(startBlock: B, maxSteps: Long): B {
        var id = startBlock.id

        while (id < startBlock.id + maxSteps) {
            val block = fetchBlock(id + 1)

            if (block != null) return block

            id++
        }

        error("Can't find next block for: $startBlock")
    }

    private suspend fun getPreviousBlock(startBlock: B): B {
        var id = startBlock.id

        while (id > 0) {
            val block = blockService.getBlock(id - 1)

            if (block != null) return block

            id--
        }

        error("Can't find previous block for: $startBlock")
    }

    private suspend fun updateBlock(block: B) {
        notifyNewBlock(block)
        blockService.save(block)
        logger.info("Block [{}:{}] saved", block.id, block.hash)
    }

    private suspend fun fetchBlock(number: Long): B? {
        val blockchainBlock = blockClient.getBlock(number)
        return blockchainBlock?.let { blockMapper.map(it) }
    }

    private suspend fun notifyNewBlock(block: B) {
        blockListener.publish(NewBlockEvent(Source.BLOCKCHAIN, block.id, block.hash))
    }

    private suspend fun notifyRevertedBlock(block: B) {
        blockListener.publish(RevertedBlockEvent(Source.BLOCKCHAIN, block.id, block.hash))
    }
}