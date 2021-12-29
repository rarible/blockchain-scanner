package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.LogRecord

abstract class TestLogRecord : LogRecord {
    abstract val id: Long
    abstract val version: Long?
    abstract val logExtra: String
    abstract val blockExtra: String
    abstract val log: TestLog
    abstract fun withIdAndVersion(id: Long, version: Long?): TestLogRecord
}

fun TestLogRecord.nullVersion() = withIdAndVersion(id = id, version = null)
