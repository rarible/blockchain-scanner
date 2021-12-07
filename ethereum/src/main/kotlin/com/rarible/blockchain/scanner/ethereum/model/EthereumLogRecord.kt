package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.LogRecord
import com.rarible.core.common.nowMillis
import java.time.Instant

abstract class EthereumLogRecord<LR : EthereumLogRecord<LR>> : LogRecord<EthereumLog, LR> {

    abstract val id: String
    abstract val version: Long?
    abstract val createdAt: Instant
    abstract val updatedAt: Instant

    abstract fun withIdAndVersion(id: String, version: Long?, updatedAt: Instant = nowMillis()): LR
}