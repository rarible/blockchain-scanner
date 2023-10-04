package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.ethereum.model.ReceivedEthereumBlock
import io.daonomic.rpc.domain.Word
import reactor.core.publisher.Flux
import scalether.core.EthPubSub

class EthereumNewBlockPubSub(
    private val ethPubSub: EthPubSub
) : EthereumNewBlockSubscriber {
    override fun newHeads(): Flux<ReceivedEthereumBlock<Word>> {
        return ethPubSub.newHeads().map { ReceivedEthereumBlock(it) }
    }
}
