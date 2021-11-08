package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import kotlinx.coroutines.flow.Flow

/**
 * Subscriber is high-level component, provided from end services, who want to gather Blockchain Log history
 * in custom format. Subscriber should provide descriptor, which used to define where to store data and how to
 * filter it. In fact, such descriptor not needed to core framework, but required for specific Blockchain
 * implementations. Also, subscriber must provide way to generate custom event data from original
 * Blockchain Block and Log.
 */
interface LogEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog, L : Log, R : LogRecord<L, *>, D : Descriptor> {

    /**
     * This descriptor will be used in Blockchain scanner implementation to define how to build various queries and
     * where to store your custom data. It will be passed to implementation of
     * [LogService][com.rarible.blockchain.scanner.framework.service.LogService],
     * [PendingLogService][com.rarible.blockchain.scanner.framework.service.PendingLogService],
     * [BlockchainClient][com.rarible.blockchain.scanner.framework.client.BlockchainClient] and
     * [LogMapper][com.rarible.blockchain.scanner.framework.mapper.LogMapper]
     */
    fun getDescriptor(): D

    /**
     * Produces custom data from single Log. All records, produced by this method will be saved lately to the
     * storage and all of them will be marked with minorLogIndex, based on order in returned flow
     * (first - 0, second - 2 etc.)
     */
    suspend fun getEventRecords(block: BB, log: BL): Flow<R>

}
