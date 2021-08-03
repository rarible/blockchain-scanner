package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.LogRecord
import org.bson.types.ObjectId

abstract class EthereumLogRecord<LR : EthereumLogRecord<LR>> : LogRecord<EthereumLog, LR> {

    abstract val id: ObjectId
    abstract val version: Long?

    abstract fun withIdAndVersion(id: ObjectId, version: Long?): LR
}