package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.block.BlockStats
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.data.BlockEvent

interface BlockEventListener<BB : BlockchainBlock> {

    suspend fun process(events: List<BlockEvent<BB>>): Map<Long, BlockStats>
}
