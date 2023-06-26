package com.rarible.blockchain.scanner.consumer

interface TransactionRecordFilter<T> {
    fun filter(event: T): Boolean
}