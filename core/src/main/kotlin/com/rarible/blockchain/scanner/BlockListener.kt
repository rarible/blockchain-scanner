package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent
import reactor.core.publisher.Mono

interface BlockListener {

    fun onBlockEvent(event: BlockEvent): Mono<Void>

}