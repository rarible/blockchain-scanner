package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.data.BlockEvent
import com.rarible.blockchain.scanner.framework.model.Log

data class ProcessedBlockEvent<L : Log>(

    val event: BlockEvent,
    val logs: List<L>
)

