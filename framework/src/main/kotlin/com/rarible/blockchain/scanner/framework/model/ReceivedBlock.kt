package com.rarible.blockchain.scanner.framework.model

import com.rarible.core.common.nowMillis
import java.time.Instant

data class ReceivedBlock<T>(
    val block: T,
    val receivedTime: Instant = nowMillis()
)
