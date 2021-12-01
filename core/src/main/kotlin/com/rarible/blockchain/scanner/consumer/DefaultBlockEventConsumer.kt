package com.rarible.blockchain.scanner.consumer

import com.rarible.blockchain.scanner.event.block.BlockListener

// TODO for intermediate compatibility, remove later
class DefaultBlockEventConsumer() : BlockEventConsumer {

    lateinit var handlers: List<BlockListener>

    override suspend fun start(handlers: Map<String, BlockListener>) {
        this.handlers = handlers.values.toList()
    }
}