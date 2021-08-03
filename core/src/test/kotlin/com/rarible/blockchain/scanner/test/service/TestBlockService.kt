package com.rarible.blockchain.scanner.test.service

import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.test.repository.TestBlockRepository
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.stereotype.Component

@Component
class TestBlockService(
    private val blockRepository: TestBlockRepository
) : BlockService<TestBlock> {

    override fun findByStatus(status: Block.Status): Flow<TestBlock> {
        return blockRepository.findByStatus(status).asFlow()
    }

    override suspend fun getLastBlockNumber(): Long {
        return blockRepository.getLastBlock().awaitFirst()
    }

    override suspend fun getBlockHash(id: Long): String {
        return blockRepository.findByIdR(id)
            .map { it.hash }
            .awaitFirst()
    }

    override suspend fun updateStatus(id: Long, status: Block.Status) {
        blockRepository.updateBlockStatus(id, status).awaitFirstOrNull()
    }

    override suspend fun save(block: TestBlock) {
        blockRepository.saveR(block).awaitFirstOrNull()
    }

}