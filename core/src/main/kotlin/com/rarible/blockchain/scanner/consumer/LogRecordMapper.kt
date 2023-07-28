package com.rarible.blockchain.scanner.consumer

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

interface LogRecordMapper<T> {
    fun map(event: T): LogRecordEvent
}
