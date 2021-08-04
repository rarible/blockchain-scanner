package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.ethereum.repository.EthereumBlockRepository
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class EthereumBlockService(
    private val blockRepository: EthereumBlockRepository
) : BlockService<EthereumBlock> {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(EthereumBlockService::class.java)
    }

    override fun findByStatus(status: Block.Status): Flow<EthereumBlock> {
        return blockRepository.findByStatus(status).asFlow()
    }

    override suspend fun getLastBlockNumber(): Long {
        return blockRepository.getLastBlock().awaitFirst()
    }

    override suspend fun getBlock(id: Long): EthereumBlock {
        return blockRepository.findByIdR(id)
            .awaitFirst()
    }

    override suspend fun updateStatus(id: Long, status: Block.Status) {
        blockRepository.updateBlockStatus(id, status).awaitFirstOrNull()
    }

    override suspend fun save(block: EthereumBlock) {
        logger.info("saveKnownBlock $block")
        blockRepository.saveR(block).awaitFirstOrNull()
    }
}