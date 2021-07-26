package com.rarible.blockchain.scanner.service.reindex

import com.rarible.blockchain.scanner.model.Block
import com.rarible.blockchain.scanner.service.BlockService
import com.rarible.blockchain.scanner.service.BlockchainListenerService
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import kotlin.math.abs

@Service
class ReindexBlockService<B : Block>(
    private val blockService: BlockService<B>,
    private val blockchainListenerService: BlockchainListenerService<*, *, B, *, *>
) {

    fun indexPendingBlocks(): Mono<Void> {
        return Flux.concat(
            blockService.findByStatus(Block.Status.PENDING)
                .filter { abs(System.currentTimeMillis() / 1000 - it.timestamp) > 60 },
            blockService.findByStatus(Block.Status.ERROR)
        )
            .concatMap { blockchainListenerService.reindexBlock(it) }
            .then()
    }
}