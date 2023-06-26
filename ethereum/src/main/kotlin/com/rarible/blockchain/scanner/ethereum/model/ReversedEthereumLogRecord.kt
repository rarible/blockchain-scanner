package com.rarible.blockchain.scanner.ethereum.model

import com.fasterxml.jackson.annotation.JsonTypeInfo
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
    val status: EthereumBlockStatus,
    val topic: Word,

    override val minorLogIndex: Int,
    val index: Int,

    val address: Address,

    val blockHash: Word? = null,
    val blockNumber: Long? = null,
    val logIndex: Int? = null,

    /**
     * Timestamp of the block (in epoch seconds).
     */
    val blockTimestamp: Long? = null,
    val from: Address? = null,
    val to: Address? = null,

    val visible: Boolean = true,

    override val createdAt: Instant = Instant.EPOCH,
    override val updatedAt: Instant = createdAt,

    val data: EventData
) : EthereumLogRecord() {

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

        blockTimestamp = blockTimestamp,
        from = from,
        to = to,
        createdAt = createdAt,
        updatedAt = updatedAt
    )

    constructor(
        id: String,
        version: Long? = null,
        log: EthereumLog,
        data: EventData
    ) : this(
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

        blockTimestamp = log.blockTimestamp,
        from = log.from,
        to = log.to,
        createdAt = log.createdAt,

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

    fun withUpdatedAt(): ReversedEthereumLogRecord {
        return copy(updatedAt = Instant.now())
    }
}

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
interface EventData {
    fun getKey(log: EthereumLog): String
}
