package com.rarible.blockchain.scanner.framework.mapper

import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor

interface LogMapper<OL, OB, L : Log> {

    fun map(
        block: OB,
        log: OL,
        index: Int,
        minorIndex: Int,
        data: EventData,
        descriptor: LogEventDescriptor
    ): L
}