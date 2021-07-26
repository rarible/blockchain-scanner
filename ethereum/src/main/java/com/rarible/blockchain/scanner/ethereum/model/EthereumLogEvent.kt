package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.model.EventData
import com.rarible.blockchain.scanner.model.LogEvent
import io.daonomic.rpc.domain.Word
import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version

data class EthereumLogEvent(
    @Id
    override val id: ObjectId,

    @Version
    override val version: Long?,

    override val topic: String,
    override val transactionHash: String,
    override val from: String? = null,
    override val nonce: Long? = null,
    override val minorLogIndex: Int,
    override val index: Int,
    override val data: EventData,

    val address: String,
    val status: LogEvent.Status,
    val blockHash: Word? = null,
    val blockNumber: Long? = null,
    val logIndex: Int? = null,
    val visible: Boolean = true

) : LogEvent