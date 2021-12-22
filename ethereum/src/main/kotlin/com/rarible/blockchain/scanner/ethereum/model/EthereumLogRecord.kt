package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.core.common.nowMillis
import java.time.Instant

abstract class EthereumLogRecord<LR : EthereumLogRecord<LR>> : LogRecord<EthereumLog, LR> {

    abstract val id: String
    abstract val version: Long?
    abstract val createdAt: Instant
    abstract val updatedAt: Instant

    /**
     * Zero-based index of this record among all records produced by subscriber from one blockchain log.
     * It is used to distinguish adjacent log records, and it is part of the composite primary key of ethereum logs,
     * which consists of transactionHash.topic.address.index.minorLogIndex
     */
    abstract val minorLogIndex: Int

    abstract fun withIdAndVersion(id: String, version: Long?, updatedAt: Instant = nowMillis()): LR

    abstract fun withLog(log: EthereumLog): LR
}
