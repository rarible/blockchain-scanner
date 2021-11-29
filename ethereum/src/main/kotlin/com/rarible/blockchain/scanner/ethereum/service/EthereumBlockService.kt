package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.ethereum.repository.EthereumBlockRepository
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.stereotype.Component

@Component
class EthereumBlockService(
    private val blockRepository: EthereumBlockRepository
) : BlockService<EthereumBlock> {

    override suspend fun getBlock(id: Long): EthereumBlock? {
        return blockRepository.findByIdR(id).awaitFirstOrNull()
    }

    override suspend fun getLastBlock(): EthereumBlock? {
        return blockRepository.getLastBlock()
    }

    override fun findByStatus(status: Block.Status): Flow<EthereumBlock> {
        return blockRepository.findByStatus(status).asFlow()
    }

    override suspend fun updateStatus(id: Long, status: Block.Status) {
        blockRepository.updateBlockStatus(id, status)
    }

    override suspend fun save(block: EthereumBlock) {
        blockRepository.saveR(block).awaitFirstOrNull()
    }

    override suspend fun remove(id: Long) {
        return blockRepository.remove(id)
    }
}