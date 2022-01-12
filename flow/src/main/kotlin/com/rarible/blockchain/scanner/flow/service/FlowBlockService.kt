package com.rarible.blockchain.scanner.flow.service

import com.rarible.blockchain.scanner.flow.model.FlowBlock
import com.rarible.blockchain.scanner.flow.repository.FlowBlockRepository
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.stereotype.Service

@Service
class FlowBlockService(
    private val blockRepository: FlowBlockRepository
): BlockService<FlowBlock> {

    override suspend fun getBlock(id: Long): FlowBlock? {
        return blockRepository.findById(id).awaitFirstOrNull()
    }

    override suspend fun updateStatus(id: Long, status: Block.Status) {
        blockRepository.updateStatus(id, status)
    }

    override suspend fun save(block: FlowBlock) {
        blockRepository.save(block).awaitSingleOrNull()
    }

    override suspend fun getLastBlock(): FlowBlock? {
        return blockRepository.findTop1ByOrderByIdDesc().awaitFirstOrNull()
    }

    override fun findByStatus(status: Block.Status): Flow<FlowBlock> {
        return blockRepository.findAllByStatus(status).asFlow()
    }
}
