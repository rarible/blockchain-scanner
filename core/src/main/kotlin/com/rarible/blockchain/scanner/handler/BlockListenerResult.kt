package com.rarible.blockchain.scanner.handler

import com.rarible.blockchain.scanner.block.BlockStats

data class BlockListenerResult(
    val blocks: List<BlockEventResult>,
    val publish: suspend () -> Unit
) {
    companion object {
        val EMPTY = BlockListenerResult(emptyList()) {}
    }
}

class BlockEventResult(
    val blockNumber: Long,
    val errors: List<BlockError>,
    val stats: BlockStats
)

data class BlockError(
    val blockNumber: Long,
    val groupId: String,
    val errorMessage: String
)
