package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.Log

data class TestLog(

    override val transactionHash: String,
    override val status: Log.Status,

    val topic: String,
    val minorLogIndex: Int,
    val blockHash: String?,
    val blockNumber: Long?,
    val logIndex: Int?,
    val index: Int,
    val extra: String,
    val visible: Boolean

) : Log<TestLog> {
    override fun withStatus(status: Log.Status): TestLog {
        return this.copy(status = status)
    }
}
