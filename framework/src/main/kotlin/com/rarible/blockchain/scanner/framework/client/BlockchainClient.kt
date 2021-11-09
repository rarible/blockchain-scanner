package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import com.rarible.blockchain.scanner.framework.model.Descriptor
import kotlinx.coroutines.flow.Flow

/**
 * Blockchain Client - implement it to support new Blockchain
 */
interface BlockchainClient<BB : BlockchainBlock, BL : BlockchainLog, D : Descriptor> {

    /**
     * Listen to new block events (poll or subscribe via websocket for example)
     */
    fun listenNewBlocks(): Flow<BB>

    /**
     * Get single block by block number
     */
    suspend fun getBlock(number: Long): BB

    /**
     * Get single block by hash
     */
    suspend fun getBlock(hash: String): BB

    /**
     * Get last known block number
     */
    suspend fun getLastBlockNumber(): Long

    /**
     * Get events from specific block and by specific descriptor
     */
    fun getBlockEvents(descriptor: D, block: BB): Flow<BL>

    /**
     * Get events from block range and by specific descriptor
     */
    fun getBlockEvents(descriptor: D, range: LongRange): Flow<FullBlock<BB, BL>>

    /**
     * Get tx meta information by transaction hash (or null if not found)
     */
    suspend fun getTransactionMeta(transactionHash: String): TransactionMeta?

}
