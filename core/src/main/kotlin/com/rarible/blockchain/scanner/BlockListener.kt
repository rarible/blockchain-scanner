package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.data.BlockEvent

interface BlockListener {

    suspend fun onBlockEvent(event: BlockEvent)

}