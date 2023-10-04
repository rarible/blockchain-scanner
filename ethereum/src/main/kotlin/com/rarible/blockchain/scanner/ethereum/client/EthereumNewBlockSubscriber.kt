package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.ethereum.model.ReceivedEthereumBlock
import io.daonomic.rpc.domain.Word
import reactor.core.publisher.Flux

interface EthereumNewBlockSubscriber {
    fun newHeads(): Flux<ReceivedEthereumBlock<Word>>
}
