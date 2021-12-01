package com.rarible.blockchain.scanner.framework.model

/**
 * Basic data class for Blockchain Log data to be stored in persistent storage as part of custom [LogRecord].
 */
interface Log<L : Log<L>> {

    val transactionHash: String
    val status: Status

    enum class Status {
        PENDING,
        CONFIRMED,

        /// TODO should not get into DB
        REVERTED,
        DROPPED,
        INACTIVE
    }

    fun withStatus(status: Status): L

}
