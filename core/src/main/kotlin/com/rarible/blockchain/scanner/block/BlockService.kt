package com.rarible.blockchain.scanner.block

import com.rarible.blockchain.scanner.event.block.Block
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
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

    suspend fun remove(id: Long) =
        blockRepository.remove(id)
}
