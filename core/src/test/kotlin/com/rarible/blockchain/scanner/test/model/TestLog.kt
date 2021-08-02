package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.Log
import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version

data class TestLog(
    @Id
    override val id: ObjectId,

    @Version
    override val version: Long?,

    override val topic: String,
    override val transactionHash: String,

    val minorLogIndex: Int,
    val data: EventData,
    val status: Log.Status,
    val blockHash: String? = null,
    val logIndex: Int? = null,
    val index: Int,
    val extra: String,
    val visible: Boolean = true

) : Log