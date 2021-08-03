package com.rarible.blockchain.scanner.test.model

class TestCustomLogRecord(
    id: Long,
    version: Long?,
    logExtra: String,
    blockExtra: String,
    log: TestLog? = null,
    val customData: String
) : TestLogRecord(
    id,
    version,
    logExtra,
    blockExtra,
    log
) {

}