package com.rarible.blockchain.scanner.ethereum.test.model

import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import com.rarible.blockchain.scanner.framework.model.Log
import io.daonomic.rpc.domain.Word
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.data.annotation.Version
import scalether.domain.Address

/**
 * Test Ethereum log similar to currently used in Ethereum-indexer.
 * It has reverted structure - Log fields placed in the root of object,
 * custom data are nested object here
 */
data class TestEthereumLogRecord(
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

    val data: TestEthereumLogData
) : EthereumLogRecord<TestEthereumLogRecord>() {

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
    )

    constructor(id: String, version: Long? = null, log: EthereumLog, data: TestEthereumLogData) : this(
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
        data = data
    )

    override fun withLog(log: EthereumLog): TestEthereumLogRecord {
        return TestEthereumLogRecord(id, version, log, data)
    }

    override fun withIdAndVersion(id: String, version: Long?): TestEthereumLogRecord {
        return copy(id = id, version = version)
    }

    override fun getKey(): String {
        return address.hex()
    }
}