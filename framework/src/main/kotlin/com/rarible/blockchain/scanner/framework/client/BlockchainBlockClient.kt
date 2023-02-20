package com.rarible.blockchain.scanner.framework.client

import kotlinx.coroutines.flow.Flow

interface BlockchainBlockClient<BB : BlockchainBlock> {

    /**
     * Listen to new block events (poll or subscribe via websocket for example)
     */
    val newBlocks: Flow<BB>

    /**
     * Get single block by block number
     */
    suspend fun getBlock(number: Long): BB?

    suspend fun getBlocks(numbers: List<Long>): List<BB>

    suspend fun getFirstAvailableBlock(): BB

    suspend fun getLastBlockNumber(): Long
}
