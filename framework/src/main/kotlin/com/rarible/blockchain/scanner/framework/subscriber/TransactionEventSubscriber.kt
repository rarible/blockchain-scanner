package com.rarible.blockchain.scanner.framework.subscriber

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.TransactionRecord

interface TransactionEventSubscriber<BB : BlockchainBlock, R : TransactionRecord> {

    fun getGroup(): String

    /**
     * Handles blockchain block
     *
     * @param block original Blockchain Block
     */
    suspend fun getEventRecords(block: BB): List<R>
}
