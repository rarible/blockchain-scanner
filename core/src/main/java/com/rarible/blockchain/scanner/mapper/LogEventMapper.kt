package com.rarible.blockchain.scanner.mapper

import com.rarible.blockchain.scanner.model.EventData
import com.rarible.blockchain.scanner.model.LogEvent
import com.rarible.blockchain.scanner.subscriber.LogEventDescriptor

interface LogEventMapper<OL, OB, L : LogEvent> {

    fun map(
        block: OB,
        log: OL,
        index: Int,
        minorIndex: Int,
        data: EventData,
        descriptor: LogEventDescriptor
    ): L
}