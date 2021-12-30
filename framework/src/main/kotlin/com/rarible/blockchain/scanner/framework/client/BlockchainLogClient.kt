package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.model.Descriptor
import kotlinx.coroutines.flow.Flow

interface BlockchainLogClient<BB : BlockchainBlock, BL : BlockchainLog, D : Descriptor> {

    /**
     * Get logs for the given [blocks].
     *
     * [stable] is false for relatively new blocks that may change.
     * Blockchains can request logs for batches of stable logs faster than for non-stable ones.
     */
    fun getBlockLogs(descriptor: D, blocks: List<BB>, stable: Boolean): Flow<FullBlock<BB, BL>>

}
