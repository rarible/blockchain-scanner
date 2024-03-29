package com.rarible.blockchain.scanner.test.model

import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version

data class TestCustomLogRecord(
    @Id
    override val id: Long,
    @Version
    override val version: Long?,
    override val logExtra: String,
    override val blockExtra: String,
    override val log: TestLog,
    val customData: String
) : TestLogRecord() {

    override fun withIdAndVersion(id: Long, version: Long?): TestCustomLogRecord = copy(id = id, version = version)

    override fun withLog(log: TestLog): TestLogRecord = copy(log = log)

    override fun getKey(): String = logExtra
}
