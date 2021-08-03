package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.LogRecord
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version

abstract class TestLogRecord(

    @Id
    var id: Long,

    @Version
    var version: Long?,

    val logExtra: String,
    val blockExtra: String,
    override var log: TestLog? = null

) : LogRecord<TestLog>