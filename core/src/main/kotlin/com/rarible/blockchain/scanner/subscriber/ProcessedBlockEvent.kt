package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

/**
 * Batch of LogEvents, gathered after entire block processed. This batch may contain events emitted by all registered
 * subscribers and also there could be logs only for one specific subscriber.
 */
data class ProcessedBlockEvent<L : Log<L>, R : LogRecord<L, *>>(

    val event: BlockEvent,
    val records: List<R>
)

