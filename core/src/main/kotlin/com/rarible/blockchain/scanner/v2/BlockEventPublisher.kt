package com.rarible.blockchain.scanner.v2

import com.rarible.blockchain.scanner.v2.event.BlockEvent

fun interface BlockEventPublisher {

    suspend fun emit(event: BlockEvent)

}