package com.rarible.blockchain.scanner.block

import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class BlockService(
    private val blockRepository: BlockRepository
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun getBlock(id: Long): Block? =
        blockRepository.findByIdR(id).awaitFirstOrNull()

    suspend fun getLastBlock(): Block? =
        blockRepository.getLastBlock()

    suspend fun save(block: Block): Block {
        val result = blockRepository.saveR(block).awaitFirst()
        logger.info("Saved block $block")
        return result
    }

    suspend fun save(blocks: List<Block>): List<Block> = blocks.map { save(it) }

    suspend fun insert(blocks: List<Block>): List<Block> =
        blockRepository.insert(blocks)

    suspend fun insertMissing(blocks: List<Block>) =
        blockRepository.insertMissing(blocks)

    suspend fun countFailed() = blockRepository.failedCount()

    suspend fun remove(id: Long) =
        blockRepository.remove(id)
}
