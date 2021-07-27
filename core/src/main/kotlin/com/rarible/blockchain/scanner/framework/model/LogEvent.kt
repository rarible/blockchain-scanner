package com.rarible.blockchain.scanner.framework.model

import com.rarible.core.common.Identifiable
import org.bson.types.ObjectId

/**
 * Basic data class for blockchain block data to be stored in Mongo
 */
interface LogEvent : Identifiable<ObjectId> {
    override val id: ObjectId
    val version: Long?

    val topic: String
    val transactionHash: String

    enum class Status {
        PENDING,
        CONFIRMED,
        REVERTED,
        DROPPED,
        INACTIVE
    }


}
