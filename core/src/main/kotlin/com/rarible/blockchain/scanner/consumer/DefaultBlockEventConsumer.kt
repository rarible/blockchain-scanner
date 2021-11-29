package com.rarible.blockchain.scanner.consumer

import com.rarible.blockchain.scanner.BlockListener

// TODO for intermediate compatibility, remove later
class DefaultBlockEventConsumer() : BlockEventConsumer {

    lateinit var handler: BlockListener

    override suspend fun start(handler: BlockListener) {
        this.handler = handler
    }
}