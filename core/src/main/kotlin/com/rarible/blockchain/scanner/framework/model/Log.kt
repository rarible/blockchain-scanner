package com.rarible.blockchain.scanner.framework.model

import com.rarible.core.common.Identifiable
import org.bson.types.ObjectId

//todo давай все-таки сделаем, чтобы в монго сохранялись кастомные event'ы сразу.
//todo И чтобы внутри было поле log: Log (где номер блока, хэш блока и т.д.)
//todo кажется, что это лучше, чем текущий подход (все равно придется переиндексировать все)
/**
 * Basic data class for blockchain block data to be stored in Mongo
 */
interface Log : Identifiable<ObjectId> {
    override val id: ObjectId
    val version: Long?

    val topic: String
    val transactionHash: String
    val status: Status

    enum class Status {
        PENDING,
        CONFIRMED,
        REVERTED,
        DROPPED,
        INACTIVE
    }

}
