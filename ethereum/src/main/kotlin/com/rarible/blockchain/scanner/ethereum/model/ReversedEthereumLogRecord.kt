package com.rarible.blockchain.scanner.ethereum.model

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.rarible.blockchain.scanner.framework.model.Log
import io.daonomic.rpc.domain.Word
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.data.annotation.Version
import scalether.domain.Address
import java.time.Instant

/**
 * Default Ethereum log similar to currently used in Ethereum-indexer.
 * It has reverted structure - Log fields placed in the root of object,
 * custom data are nested object here
 */
data class ReversedEthereumLogRecord(
    @Id
    override val id: String,
    @Version
    override val version: Long? = null,

    val transactionHash: String,
    val status: Log.Status,
    val topic: Word,

    val minorLogIndex: Int,
    val index: Int,

    val address: Address,

    val blockHash: Word? = null,
    val blockNumber: Long? = null,
    val logIndex: Int? = null,

    val visible: Boolean = true,

    override val createdAt: Instant = Instant.EPOCH,
    override val updatedAt: Instant = Instant.EPOCH,

    val data: EventData
) : EthereumLogRecord<ReversedEthereumLogRecord>() {

    @Transient
    override val log: EthereumLog = EthereumLog(
        transactionHash = transactionHash,
        status = status,
        topic = topic,

        minorLogIndex = minorLogIndex,
        index = index,

        address = address,

        blockHash = blockHash,
        blockNumber = blockNumber,
        logIndex = logIndex,

        visible = visible,

        createdAt = createdAt,
        updatedAt = updatedAt
    )

    constructor(id: String, version: Long? = null, log: EthereumLog, data: EventData) : this(
        id = id,
        version = version,

        transactionHash = log.transactionHash,
        status = log.status,
        topic = log.topic,

        minorLogIndex = log.minorLogIndex,
        index = log.index,

        address = log.address,

        blockHash = log.blockHash,
        blockNumber = log.blockNumber,
        logIndex = log.logIndex,

        visible = log.visible,

        createdAt = log.createdAt,
        updatedAt = log.updatedAt,

        data = data
    )

    override fun withLog(log: EthereumLog): ReversedEthereumLogRecord {
        return ReversedEthereumLogRecord(id, version, log, data)
    }

    override fun withIdAndVersion(id: String, version: Long?, updatedAt: Instant): ReversedEthereumLogRecord {
        return copy(id = id, version = version, updatedAt = updatedAt)
    }

    override fun getKey(): String {
        return data.getKey(log)
    }

}

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
interface EventData {
    fun getKey(log: EthereumLog): String
}