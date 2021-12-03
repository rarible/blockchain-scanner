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
    override val log: TestLog? = null,
    val customData: String
) : TestLogRecord<TestCustomLogRecord>() {

    override fun withLog(log: TestLog): TestCustomLogRecord {
        return copy(log = log)
    }

    override fun withIdAndVersion(id: Long, version: Long?): TestCustomLogRecord {
        return copy(id = id, version = version)
    }

    override fun getKey(): String {
        return logExtra
    }

    override fun getTopic(): String {
        return "test"
    }
}