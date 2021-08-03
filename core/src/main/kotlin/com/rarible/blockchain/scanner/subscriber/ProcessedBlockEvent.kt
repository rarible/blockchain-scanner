package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

data class ProcessedBlockEvent<L : Log>(

    val event: BlockEvent,
    val records: List<LogRecord<L>>
)

