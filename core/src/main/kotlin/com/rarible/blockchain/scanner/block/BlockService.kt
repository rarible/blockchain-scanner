package com.rarible.blockchain.scanner.block

import com.rarible.core.common.optimisticLock
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.TreeMap

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
        blockRepository.insertAll(blocks).toList()

    suspend fun insertMissing(blocks: List<Block>) = optimisticLock {
        val exist = blockRepository.getByIds(blocks.map { it.id }).associateByTo(TreeMap()) { it.id }
        val toInsert = blocks.filter { !exist.contains(it.id) }
        val inserted = insert(toInsert)
        inserted.forEach { exist[it.id] = it }
        exist.values.toList()
    }

    suspend fun countFailed() = blockRepository.failedCount()

    suspend fun remove(id: Long) =
        blockRepository.remove(id)
}
