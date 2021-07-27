package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import reactor.core.publisher.Flux

interface BlockScanner {

    fun scan(): Flux<BlockEvent>

}