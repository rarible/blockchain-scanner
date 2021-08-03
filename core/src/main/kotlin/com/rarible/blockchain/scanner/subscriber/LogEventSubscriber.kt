package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import kotlinx.coroutines.flow.Flow

interface LogEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog, L : Log, R : LogRecord<L>, D : Descriptor> {

    fun getDescriptor(): D

    fun getEventRecords(block: BB, log: BL): Flow<R>

}