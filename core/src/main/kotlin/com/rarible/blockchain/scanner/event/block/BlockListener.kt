package com.rarible.blockchain.scanner.event.block

import com.rarible.blockchain.scanner.framework.data.BlockEvent

interface BlockListener {

    suspend fun onBlockEvents(events: List<BlockEvent>)

}
