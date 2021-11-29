package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.BlockEvent

class KafkaBlockEventPublisher : BlockEventPublisher {

    override suspend fun publish(event: BlockEvent) {

    }
}