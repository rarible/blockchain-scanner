package com.rarible.blockchain.scanner.test.service

import com.rarible.blockchain.scanner.framework.service.BlockService
import com.rarible.blockchain.scanner.test.model.TestBlock
import com.rarible.blockchain.scanner.test.repository.TestBlockRepository
import kotlinx.coroutines.reactive.awaitFirstOrNull

class TestBlockService(
    private val blockRepository: TestBlockRepository
) : BlockService<TestBlock> {

    override suspend fun getBlock(id: Long): TestBlock? {
        return blockRepository.findByIdR(id)
            .awaitFirstOrNull()
    }

    override suspend fun getLastBlock(): TestBlock? {
        return blockRepository.getLastBlock().awaitFirstOrNull()
    }

    override suspend fun save(block: TestBlock) {
        blockRepository.saveR(block).awaitFirstOrNull()
    }

    override suspend fun remove(id: Long) {
        blockRepository.remove(id)
    }
}