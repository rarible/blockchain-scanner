package com.rarible.blockchain.scanner.ethereum.service

import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import com.rarible.blockchain.scanner.ethereum.repository.EthereumBlockRepository
import com.rarible.blockchain.scanner.framework.model.Block
import com.rarible.blockchain.scanner.framework.service.BlockService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Component
class EthereumBlockService(
    private val blockRepository: EthereumBlockRepository
) : BlockService<EthereumBlock> {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(EthereumBlockService::class.java)
    }

    override fun findByStatus(status: Block.Status): Flux<EthereumBlock> {
        return blockRepository.findByStatus(status)
    }

    override fun getLastBlock(): Mono<Long> {
        return blockRepository.getLastBlock()
    }

    override fun getBlockHash(id: Long): Mono<String> {
        return blockRepository.findByIdR(id)
            .map { it.hash }
    }

    override fun updateBlockStatus(id: Long, status: Block.Status): Mono<Void> {
        return blockRepository.updateBlockStatus(id, status)
    }

    override fun saveBlock(block: EthereumBlock): Mono<Void> {
        logger.info("saveKnownBlock $block")
        return blockRepository.saveR(block).then()
    }

    override fun findFirstByIdAsc(): Mono<EthereumBlock> {
        return blockRepository.findFirstByIdAsc()
    }

    override fun findFirstByIdDesc(): Mono<EthereumBlock> {
        return blockRepository.findFirstByIdDesc()
    }
}