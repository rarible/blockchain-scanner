package com.rarible.blockchain.scanner.v2

import com.rarible.blockchain.scanner.BlockListener
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainBlockClient
import com.rarible.blockchain.scanner.framework.mapper.BlockMapper
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import kotlinx.coroutines.flow.FlowCollector
import org.slf4j.LoggerFactory

class BlockHandler<BB : BlockchainBlock, B : Block>(
    private val blockMapper: BlockMapper<BB, B>,
    private val blockClient: BlockchainBlockClient<BB>,
    private val blockService: BlockService<B>,
    private val emitter: FlowCollector<BlockEvent>
) {

    private val logger = LoggerFactory.getLogger(BlockListener::class.java)

    suspend fun onNewBlock(newBlockchainBlock: BB) {
        val lastKnownBlock = getLastKnownBlock()
        val lastBlockchainBlock = blockClient.getBlock(lastKnownBlock.id)

        var blockchainBlock = blockMapper.map(lastBlockchainBlock)
        var stateBlock = checkAndRevert(lastKnownBlock, blockchainBlock)

        val newBlock = blockMapper.map(newBlockchainBlock)
        while (blockchainBlock.id < newBlock.id) {
            val nextBlockchainBlock = blockClient.getBlock(blockchainBlock.id + 1) ?: break
            while (nextBlockchainBlock.parentHash != stateBlock.hash) {
                // chain reorg happened, revert blocks and update state
                val parentHash = nextBlockchainBlock.parentHash
                    ?: error("never happens: parent hash of the next block should not be blank")
                val prevBlockchainBlock = blockClient.getBlock(parentHash)
                    ?: error("never happens: block with hash $parentHash not found in the blockchain")
                stateBlock = checkAndRevert(stateBlock, blockMapper.map(prevBlockchainBlock)) //status = PENDING
            }

            blockchainBlock = blockMapper.map(nextBlockchainBlock) // status = PENDING
            emitter.emit(NewBlockEvent(blockchainBlock.id, blockchainBlock.hash))
            blockService.save(blockchainBlock)
            stateBlock = blockchainBlock
        }
    }

    private suspend fun getLastKnownBlock(): B {
        val lastKnownBlock = blockService.getLastBlock()
        if (lastKnownBlock != null) {
            return lastKnownBlock
        }
        logger.info("There is no last know block, retrieving first one")
        val blockchainBlock = blockClient.getBlock(0)
        val block = blockMapper.map(blockchainBlock)

        logger.info("Found first block in chain: {}", block)
        blockService.save(block)
        notifyNewBlock(block)
        return block
    }


    /**
     * Checks if is needed to revert blocks (by comparing block hashes)
     * @param startStateBlock block saved in the state currently
     * @param startBlockchainBlock block got from the blockchain with the save block number
     * @return correct block saved into the state after reverting
     */
    private suspend fun checkAndRevert(startStateBlock: B, startBlockchainBlock: B): B {
        val startBlockPair = BlockPair(startStateBlock, startBlockchainBlock)
        val reverted = findBlocksToRevert(startBlockPair)
        reverted.forEach { revertBlock(it) }
        return reverted.lastOrNull()?.blockchainBlock ?: startStateBlock
    }

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

        val parentStateBlock = blockService.getBlock(blockPair.number)
        // Should never happen
            ?: error("Block with number ${blockPair.number} not found in state")

        if (blockchainBlock.parentHash == null) {
            // Should never happen
            error("Root block reached, child block was: #${blockchainBlock.id}, hash=${blockchainBlock.hash}")
        }

        val blockchainParentBlock = blockClient.getBlock(blockchainBlock.parentHash!!)
        return BlockPair(parentStateBlock, blockMapper.map(blockchainParentBlock))
    }

    private suspend fun revertBlock(blockPair: BlockPair<B>) {
        val revertedBlock = blockPair.stateBlock
        blockService.save(blockPair.blockchainBlock)
        notifyRevertedBlock(revertedBlock)
        logger.info(
            "Block reverted #{}, hash: {} -> {}",
            revertedBlock.id, revertedBlock.hash, blockPair.blockchainBlock.hash
        )
    }

    private suspend fun notifyNewBlock(block: B) {
        emitter.emit(NewBlockEvent(block.id, block.hash))
    }

    private suspend fun notifyRevertedBlock(block: B) {
        emitter.emit(RevertedBlockEvent(block.id, block.hash))
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