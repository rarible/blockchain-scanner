package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

data class ProcessedBlockEvent<L : Log, R : LogRecord<L, *>>(

    val event: BlockEvent,
    val records: List<R>
)

