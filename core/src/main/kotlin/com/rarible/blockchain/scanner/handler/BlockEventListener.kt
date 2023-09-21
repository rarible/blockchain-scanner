package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.block.BlockStats
import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.data.BlockEvent

interface BlockEventListener<BB : BlockchainBlock> {

    suspend fun process(events: List<BlockEvent<BB>>): Result

    data class Result(
        val stats: Map<Long, BlockStats>,
        val publish: suspend () -> Unit
    ) {
        companion object {
            val EMPTY = Result(emptyMap()) {}
        }
    }
}
