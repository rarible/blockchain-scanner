package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.model.ReceivedBlock
import io.daonomic.rpc.domain.Word
import reactor.core.publisher.Flux
import scalether.core.EthPubSub
import scalether.domain.response.Block

class EthereumNewBlockPubSub(
    private val ethPubSub: EthPubSub
) : EthereumNewBlockSubscriber {
    override fun newHeads(): Flux<ReceivedBlock<Block<Word>>> {
        return ethPubSub.newHeads().map { ReceivedBlock(it) }
    }
}
