package com.rarible.blockchain.scanner.event.block

import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.stereotype.Component

@Component
class BlockService(
    private val blockRepository: BlockRepository
) {

    suspend fun getBlock(id: Long): Block? {
        return blockRepository.findByIdR(id).awaitFirstOrNull()
    }

    suspend fun getLastBlock(): Block? {
        return blockRepository.getLastBlock()
    }

    suspend fun save(block: Block) {
        blockRepository.saveR(block).awaitFirstOrNull()
    }

    suspend fun remove(id: Long) {
        return blockRepository.remove(id)
    }
}