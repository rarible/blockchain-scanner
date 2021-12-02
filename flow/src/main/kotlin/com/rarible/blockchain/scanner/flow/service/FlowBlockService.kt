package com.rarible.blockchain.scanner.flow.service

import com.rarible.blockchain.scanner.flow.model.FlowBlock
import com.rarible.blockchain.scanner.flow.repository.FlowBlockRepository
import com.rarible.blockchain.scanner.framework.service.BlockService
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.stereotype.Service

@Service
class FlowBlockService(
    private val blockRepository: FlowBlockRepository
): BlockService<FlowBlock> {

    override suspend fun getBlock(id: Long): FlowBlock? {
        return blockRepository.findById(id).awaitFirstOrNull()
    }

    override suspend fun save(block: FlowBlock) {
        blockRepository.save(block).subscribe()
    }

    override suspend fun getLastBlock(): FlowBlock? {
        return blockRepository.findTop1ByOrderByIdDesc().awaitFirstOrNull()
    }

    override suspend fun remove(id: Long) {
        blockRepository.deleteById(id).awaitFirstOrNull()
    }
}
