package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.consumer.DefaultBlockEventConsumer
import com.rarible.blockchain.scanner.framework.data.BlockEvent

// TODO for intermediate compatibility, remove later
class DefaultBlockEventPublisher(
    private val consumer: DefaultBlockEventConsumer
) : BlockEventPublisher {

    override suspend fun publish(event: BlockEvent) {
        consumer.handler.onBlockEvents(listOf(event))
    }
}