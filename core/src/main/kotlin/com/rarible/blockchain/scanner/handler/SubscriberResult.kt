package com.rarible.blockchain.scanner.handler

sealed class SubscriberResult<T> {
    abstract val blockNumber: Long
    abstract val descriptorId: String
}

data class SubscriberResultOk<T>(
    override val blockNumber: Long,
    override val descriptorId: String,
    val result: T
) : SubscriberResult<T>()

data class SubscriberResultFail<T>(
    override val blockNumber: Long,
    override val descriptorId: String,
    val errorMessage: String
) : SubscriberResult<T>()
