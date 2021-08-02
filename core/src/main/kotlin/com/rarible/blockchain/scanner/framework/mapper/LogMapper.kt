package com.rarible.blockchain.scanner.framework.mapper

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor

interface LogMapper<BB : BlockchainBlock, BL : BlockchainLog, L : Log> {

    fun map(
        block: BB,
        log: BL,
        index: Int,
        minorIndex: Int,
        data: EventData,
        descriptor: LogEventDescriptor
    ): L
}