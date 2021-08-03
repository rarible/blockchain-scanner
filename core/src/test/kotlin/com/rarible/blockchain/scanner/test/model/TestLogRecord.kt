package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.LogRecord

abstract class TestLogRecord<LR : TestLogRecord<LR>> : LogRecord<TestLog, LR> {
    abstract val id: Long
    abstract val version: Long?
    abstract val logExtra: String
    abstract val blockExtra: String

    abstract fun withIdAndVersion(id: Long, version: Long?): LR
}

