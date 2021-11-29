package com.rarible.blockchain.scanner

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow

class NewBlockScanner(
    private val client: BlockClient,
    private val blockService: BlockService
) {
    val blockEvents: Flow<BlockEvent> = flow {
        suspend fun getLastKnownBlockOrInsertRoot(): Block {
            val lastKnown = blockService.getLastKnownBlock()
            if (lastKnown == null) {
                val block = client.getBlockByNumber(0) ?: error("no root block")
                emit(BlockEvent.NewBlockEvent(block.number, block.hash))
                blockService.saveBlock(block)
                return block
            }
            return lastKnown
        }

        /**
         * Checks if it's needed to revert any blocks
         * @param startStateBlock the latest block from the state
         * @param startBlockchainBlock block from the blockchain with the same number
         * @return list of pairs (state block, blockchain block) to revert. blocks are sorted from new to old
         */
        suspend fun findBlocksToRevert(
            startStateBlock: Block,
            startBlockchainBlock: Block
        ): MutableList<Pair<Block, Block>> {
            assert(startBlockchainBlock.number == startStateBlock.number)
            val revert = mutableListOf<Pair<Block, Block>>()
            var stateBlock = startStateBlock
            var blockchainBlock = startBlockchainBlock
            while (stateBlock.hash != blockchainBlock.hash) {
                revert.add(stateBlock to blockchainBlock)
                stateBlock = blockService.getBlockByNumber(stateBlock.number - 1)
                    ?: error("never happens: block with number ${stateBlock.number - 1} not found in state")
                blockchainBlock =
                    client.getBlockByHash(
                        blockchainBlock.parentHash ?: throw IllegalStateException("root block reached")
                    )
                        ?: error("never happens: block with hash ${blockchainBlock.parentHash} not found in the blockchain")
            }
            return revert
        }

        /**
         * Checks if it's needed to revert blocks (by comparing block hashes)
         * @param startStateBlock block saved in the state currently
         * @param startBlockchainBlock block got from the blockchain with the save block number
         * @return correct block saved into the state after reverting
         */
        suspend fun checkAndRevert(startStateBlock: Block, startBlockchainBlock: Block): Block {
            val toRevert = findBlocksToRevert(startStateBlock, startBlockchainBlock)
            return if (toRevert.isNotEmpty()) {
                toRevert.forEach { (state, _) ->
                    emit(BlockEvent.RevertedBlockEvent(state))
                    blockService.removeBlock(state)
                }
                toRevert.asReversed().forEach { (_, blockchain) ->
                    emit(BlockEvent.NewBlockEvent(blockchain))
                    blockService.saveBlock(blockchain)
                }
                toRevert.first().second
            } else {
                startStateBlock
            }
        }

        suspend fun onNewBlock(newBlock: Block) {
            val lastKnownBlock = getLastKnownBlockOrInsertRoot()
            val fromBlockchain = client.getBlockByNumber(lastKnownBlock.number)
                ?: error("never happens: block with number ${lastKnownBlock.number} not found in blockchain")

            var stateBlock = checkAndRevert(lastKnownBlock, fromBlockchain)
            var blockchainBlock = fromBlockchain

            while (blockchainBlock.number < newBlock.number) {
                val nextBlockchainBlock = client.getBlockByNumber(blockchainBlock.number + 1) ?: break
                while (nextBlockchainBlock.parentHash != stateBlock.hash) {
                    // chain reorg happend, revert blocks and update state
                    val parentHash = nextBlockchainBlock.parentHash
                        ?: error("never happens: parent hash of the next block should not be blank")
                    val prevBlockchainBlock = client.getBlockByHash(parentHash)
                        ?: error("never happens: block with hash $parentHash not found in the blockchain")
                    stateBlock = checkAndRevert(stateBlock, prevBlockchainBlock)
                }

                blockchainBlock = nextBlockchainBlock
                emit(BlockEvent.NewBlockEvent(blockchainBlock))
                blockService.saveBlock(blockchainBlock)
                stateBlock = blockchainBlock
            }
        }

        client.newBlocks.collect {
            onNewBlock(it)
        }
    }

}

sealed class BlockEvent {
    data class NewBlockEvent(val number: Long, val hash: String) : BlockEvent() {
        constructor(block: Block) : this(block.number, block.hash)
    }

    data class RevertedBlockEvent(val number: Long, val hash: String) : BlockEvent() {
        constructor(block: Block) : this(block.number, block.hash)
    }
}

data class Block(
    val number: Long,
    val hash: String,
    val parentHash: String?
)

interface BlockClient {
    val newBlocks: Flow<Block>
    suspend fun getBlockByHash(hash: String): Block?
    suspend fun getBlockByNumber(number: Long): Block?
}

interface BlockService {
    suspend fun getLastKnownBlock(): Block?
    suspend fun getBlockByNumber(blockNumber: Long): Block?
    suspend fun saveBlock(block: Block)
    suspend fun removeBlock(block: Block)
}
