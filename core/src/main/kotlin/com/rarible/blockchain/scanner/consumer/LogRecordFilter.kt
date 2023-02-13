package com.rarible.blockchain.scanner.consumer

interface LogRecordFilter<T> {
    fun filter(event: T): Boolean
}