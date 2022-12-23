package com.rarible.blockchain.scanner.ethereum.client

import io.daonomic.rpc.domain.Word
import reactor.core.publisher.Flux
import scalether.core.EthPubSub
import scalether.domain.response.Block

class EthereumNewBlockPubSub(
    private val ethPubSub: EthPubSub
) : EthereumNewBlockSubscriber {
    override fun newHeads(): Flux<Block<Word>> = ethPubSub.newHeads()
}