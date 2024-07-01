package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.model.ReceivedBlock
import io.daonomic.rpc.domain.Word
import kotlinx.coroutines.flow.Flow
import reactor.core.publisher.Flux
import scalether.domain.response.Block

interface EthereumNewBlockSubscriber {
    fun newHeads(): Flow<ReceivedBlock<Block<Word>>>
    fun newHeadsAsFlux(): Flux<ReceivedBlock<Block<Word>>>
}
