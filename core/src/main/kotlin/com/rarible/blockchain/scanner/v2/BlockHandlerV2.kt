package com.rarible.blockchain.scanner.v2

import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.v2.event.NewBlockEvent
import com.rarible.blockchain.scanner.v2.event.RevertedBlockEvent
import org.slf4j.LoggerFactory

class BlockHandlerV2<BB : BlockchainBlock, B : Block>(
    private val blockMapper: BlockMapper<BB, B>,
    private val blockClient: BlockchainBlockClient<BB>,
    private val blockService: BlockService<B>,
    private val publisher: BlockEventPublisher
) {

    private val logger = LoggerFactory.getLogger(BlockListener::class.java)

    suspend fun onNewBlock(newBlockchainBlock: BB) {
        logger.info("Received new Blockchain Block: #{}", newBlockchainBlock.number)

        val lastKnownStateBlock = getLastKnownBlock()
        logger.info("Last known block: #{}", newBlockchainBlock.number)

        var blockchainBlock = fetchBlock(lastKnownStateBlock.id)

        // Current "head" in DB
        var stateBlock = checkAndRevert(lastKnownStateBlock, blockchainBlock)

        while (blockchainBlock.id < newBlockchainBlock.number) {
            val nextBlockNumber = blockchainBlock.id + 1
            val nextBlockchainBlock = fetchBlock(nextBlockNumber)
            val expectedParentHash = nextBlockchainBlock.parentHash!!

            // There could be situation when blockchain reorganized while we're handling events,
            // so checking chain consistency for all forward-going blocks
            while (expectedParentHash != stateBlock.hash) {
                // In such case we have head block in DB with wrong hash, reverting it
                logger.info(
                    "Found Block #{} with changed hash: {} -> {}",
                    stateBlock.id, stateBlock.hash, expectedParentHash
                )

                val prevBlockchainBlock = fetchBlock(expectedParentHash)
                stateBlock = checkAndRevert(stateBlock, prevBlockchainBlock)
            }

            blockchainBlock = nextBlockchainBlock
            stateBlock = blockchainBlock

            updateBlock(blockchainBlock)
        }
    }

    private suspend fun getLastKnownBlock(): B {
        val lastKnownBlock = blockService.getLastBlock()
        if (lastKnownBlock != null) {
            return lastKnownBlock
        }
        logger.info("There is no last know block, retrieving first one")
        val blockchainBlock = fetchBlock(0)

        logger.info("Found first block in chain: {}", blockchainBlock)
        updateBlock(blockchainBlock)
        return blockchainBlock
    }


    /**
     * Checks if is needed to revert blocks (by comparing block hashes)
     * @param startStateBlock block saved in the state currently
     * @param startBlockchainBlock block got from the blockchain with the save block number
     * @return correct block saved into the state after reverting
     */
    private suspend fun checkAndRevert(startStateBlock: B, startBlockchainBlock: B): B {
        val startBlockPair = BlockPair(startStateBlock, startBlockchainBlock)
        // Ordered number DESC
        val reverted = findBlocksToRevert(startBlockPair)
        // Deleting reverted blocks in DESC order
        reverted.forEach { revertBlock(it) }
        // Inserting actual blocks in ASC order
        reverted.reversed().forEach { updateBlock(it.blockchainBlock) }

        return reverted.firstOrNull()?.blockchainBlock ?: startStateBlock
    }

    /**
     * Checks if it's needed to revert any blocks
     * @param startBlockPair the latest block pair from the state
     * @return list of pairs (state block, blockchain block) to revert. blocks are sorted from new to old
     */
    private suspend fun findBlocksToRevert(startBlockPair: BlockPair<B>): MutableList<BlockPair<B>> {
        val toRevert = mutableListOf<BlockPair<B>>()
        var blockPair = startBlockPair
        while (!blockPair.isSameHash()) {
            toRevert.add(blockPair)
            blockPair = getParentBlockPair(blockPair)
        }
        return toRevert
    }

    private suspend fun getParentBlockPair(blockPair: BlockPair<B>): BlockPair<B> {
        val blockchainBlock = blockPair.blockchainBlock

        val parentStateBlock = blockService.getBlock(blockPair.number - 1)
        // Should never happen
            ?: error("Block with number ${blockPair.number} not found in state")

        if (blockchainBlock.parentHash == null) {
            // Should never happen
            error("Root block reached, child block was: #${blockchainBlock.id}, hash=${blockchainBlock.hash}")
        }

        val blockchainParentBlock = fetchBlock(blockchainBlock.parentHash!!)
        return BlockPair(parentStateBlock, blockchainParentBlock)
    }

    private suspend fun revertBlock(blockPair: BlockPair<B>) {
        val reverted = blockPair.stateBlock
        val updated = blockPair.blockchainBlock
        blockService.remove(updated.id)
        logger.info("Block #{} reverted, hash: {} -> {}", reverted.id, reverted.hash, updated.hash)
        notifyRevertedBlock(reverted)
    }

    private suspend fun updateBlock(block: B) {
        blockService.save(block)
        logger.info("Block #{} saved, hash={}, ts={}", block.id, block.hash, block.timestamp)
        notifyNewBlock(block)
    }

    private suspend fun fetchBlock(number: Long): B {
        val blockchainBlock = blockClient.getBlock(number)
        return blockMapper.map(blockchainBlock)
    }

    private suspend fun fetchBlock(hash: String): B {
        val blockchainBlock = blockClient.getBlock(hash)
        return blockMapper.map(blockchainBlock)
    }

    private suspend fun notifyNewBlock(block: B) {
        publisher.emit(NewBlockEvent(block.id, block.hash))
    }

    private suspend fun notifyRevertedBlock(block: B) {
        publisher.emit(RevertedBlockEvent(block.id, block.hash))
    }

    private data class BlockPair<B : Block>(
        val stateBlock: B,
        val blockchainBlock: B
    ) {

        val number = stateBlock.id

        fun isSameHash(): Boolean {
            return stateBlock.hash == blockchainBlock.hash
        }
    }

}