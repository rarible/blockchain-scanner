package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.ethereum.repository.EthereumBlockRepository
import com.rarible.blockchain.scanner.framework.service.BlockService
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

    override suspend fun save(block: EthereumBlock) {
        blockRepository.saveR(block).awaitFirstOrNull()
    }

    override suspend fun remove(id: Long) {
        return blockRepository.remove(id)
    }
}