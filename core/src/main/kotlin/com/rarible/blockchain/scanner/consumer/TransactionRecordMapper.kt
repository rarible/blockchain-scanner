package com.rarible.blockchain.scanner.consumer

import com.rarible.blockchain.scanner.framework.data.TransactionRecordEvent

interface TransactionRecordMapper<T> {
    fun map(event: T): TransactionRecordEvent
}