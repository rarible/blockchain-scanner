package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.framework.data.BlockEvent

interface BlockEventListener {
    suspend fun process(events: List<BlockEvent>)
}
