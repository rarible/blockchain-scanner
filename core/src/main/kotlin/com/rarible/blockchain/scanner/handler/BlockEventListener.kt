package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.data.ScanMode

interface BlockEventListener<BB : BlockchainBlock> {

    val groupId: String

    suspend fun process(events: List<BlockEvent<BB>>, mode: ScanMode): BlockListenerResult
}
