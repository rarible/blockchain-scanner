package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.EventData
import kotlinx.coroutines.flow.Flow

interface LogEventSubscriber<BB : BlockchainBlock, BL : BlockchainLog, D : Descriptor> {

    fun getDescriptor(): D

    fun getEventData(block: BB, log: BL): Flow<EventData>

}