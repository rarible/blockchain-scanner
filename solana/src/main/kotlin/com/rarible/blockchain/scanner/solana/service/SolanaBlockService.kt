package com.rarible.blockchain.scanner.solana.service

import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.solana.model.SolanaBlock
import com.rarible.blockchain.scanner.solana.repository.SolanaBlockRepository
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.stereotype.Component

@Component
class SolanaBlockService(
    private val blockRepository: SolanaBlockRepository
) : BlockService<SolanaBlock> {
    override suspend fun getBlock(id: Long): SolanaBlock? {
        return blockRepository.findByIdR(id).awaitFirstOrNull()
    }

    override suspend fun getLastBlock(): SolanaBlock? {
        return blockRepository.getLastBlock()
    }

    override suspend fun save(block: SolanaBlock) {
        blockRepository.saveR(block).awaitFirstOrNull()
    }

    override suspend fun remove(id: Long) {
        return blockRepository.remove(id)
    }
}