package com.rarible.blockchain.scanner.framework.subscriber

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord

/**
 * Subscriber is high-level component, provided from end services, who want to gather Blockchain Log history
 * in custom format. Subscriber should provide descriptor, which used to define where to store data and how to
 * filter it. In fact, such descriptor not needed to core framework, but required for specific Blockchain
 * implementations. Also, subscriber must provide way to generate custom event data from original
 * Blockchain Block and Log.
 */

// Inside of group logs should be processed together
interface LogEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog, R : LogRecord, D : Descriptor> {

    /**
     * Descriptor used to define where to store data and how to serialize/deserialize it.
     */
    fun getDescriptor(): D

    /**
     * Produces custom data from single Log.
     *
     * @param block original Blockchain Block
     * @param log original Blockchain Log
     */
    suspend fun getEventRecords(block: BB, log: BL): List<R>

}
