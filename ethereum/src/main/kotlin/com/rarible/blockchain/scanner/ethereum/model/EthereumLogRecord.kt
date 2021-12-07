package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.LogRecord

abstract class EthereumLogRecord<LR : EthereumLogRecord<LR>> : LogRecord<EthereumLog, LR> {

    abstract val id: String
    abstract val version: Long?

    abstract fun withIdAndVersion(id: String, version: Long?): LR
}