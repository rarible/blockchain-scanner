package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.framework.data.FullBlock
import com.rarible.blockchain.scanner.framework.data.TransactionMeta
import com.rarible.blockchain.scanner.framework.model.Descriptor
import kotlinx.coroutines.flow.Flow

interface BlockchainLogClient<BB : BlockchainBlock, BL : BlockchainLog, D : Descriptor> {

    /**
     * Get logs from block range and by specific descriptor
     */
    fun getBlockLogs(descriptor: D, range: LongRange): Flow<FullBlock<BB, BL>>

    /**
     * Get tx meta information by transaction hash (or null if not found)
     */
    suspend fun getTransactionMeta(transactionHash: String): TransactionMeta?

}
