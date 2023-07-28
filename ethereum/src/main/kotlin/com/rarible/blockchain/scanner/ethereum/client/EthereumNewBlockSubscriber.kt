package com.rarible.blockchain.scanner.ethereum.client

import io.daonomic.rpc.domain.Word
import reactor.core.publisher.Flux
import scalether.domain.response.Block

interface EthereumNewBlockSubscriber {
    fun newHeads(): Flux<Block<Word>>
}
