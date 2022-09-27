package com.rarible.blockchain.scanner.block

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Component

@Component
class BlockService(
    private val blockRepository: BlockRepository
) {

    suspend fun getBlock(id: Long): Block? =
        blockRepository.findByIdR(id).awaitFirstOrNull()

    suspend fun getLastBlock(): Block? =
        blockRepository.getLastBlock()

    suspend fun save(block: Block): Block =
        blockRepository.saveR(block).awaitFirst()

    suspend fun insertAll(blocks: List<Block>): List<Block> =
        blockRepository.insertAll(blocks).toList()

    suspend fun insertMissing(blocks: List<Block>): List<Block> {
        val exist = blockRepository.getByIds(blocks.map { it.id }).map { it.id }.toSet()
        val toInsert = blocks.filter { !exist.contains(it.id) }
        try {
            insertAll(toInsert)
        } catch (e: DuplicateKeyException) {
            // There potentially can be potential - in such case, fallback to one-by-one save
            toInsert.forEach { save(it) }
        }
        // Ideally we should return here mix of saved/existing entities, but not sure if it's really needed
        return blocks
    }

    suspend fun remove(id: Long) =
        blockRepository.remove(id)
}
