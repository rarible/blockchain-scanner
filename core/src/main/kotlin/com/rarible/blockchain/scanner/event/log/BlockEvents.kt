package com.rarible.blockchain.scanner.event.log

import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.LogRecord

data class BlockEvents(
    val blockEvent: BlockEvent,
    val subscriberLogs: Map<Descriptor, List<LogRecord<*, *>>>
)