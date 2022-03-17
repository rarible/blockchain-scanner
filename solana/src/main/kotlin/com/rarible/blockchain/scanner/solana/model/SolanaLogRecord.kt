package com.rarible.blockchain.scanner.solana.model

import com.rarible.blockchain.scanner.framework.model.LogRecord
import org.springframework.data.annotation.AccessType
import org.springframework.data.annotation.Id

abstract class SolanaLogRecord : LogRecord {
    @get:Id
    @get:AccessType(AccessType.Type.PROPERTY)
    var id: String
        get() = log.stringValue
        set(_) {}

    abstract val log: SolanaLog
}
