package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.BlockEvent


fun interface BlockEventPublisher {

    suspend fun publish(event: BlockEvent)

}