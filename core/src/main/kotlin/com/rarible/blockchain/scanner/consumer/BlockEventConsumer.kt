package com.rarible.blockchain.scanner.consumer

import com.rarible.blockchain.scanner.event.block.BlockListener

interface BlockEventConsumer {

    suspend fun start(handler: BlockListener)

}