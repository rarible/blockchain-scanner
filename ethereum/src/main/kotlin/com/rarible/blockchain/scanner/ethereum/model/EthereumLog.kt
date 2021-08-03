package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.EventData
import com.rarible.blockchain.scanner.framework.model.Log
import io.daonomic.rpc.domain.Word
import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version

data class EthereumLog(
    @Id
    override val id: ObjectId,

    @Version
    override val version: Long?,

    val topic: String,
    override val transactionHash: String,
    override val status: Log.Status,

    val from: String? = null,
    val nonce: Long? = null,
    val minorLogIndex: Int,
    val index: Int,
    val data: EventData,
    val address: String,
    val blockHash: Word? = null,
    val blockNumber: Long? = null,
    val logIndex: Int? = null,
    val visible: Boolean = true

) : Log