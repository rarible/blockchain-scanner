package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.framework.data.BlockEvent

interface BlockListener {

    suspend fun onBlockEvent(event: BlockEvent)

}
