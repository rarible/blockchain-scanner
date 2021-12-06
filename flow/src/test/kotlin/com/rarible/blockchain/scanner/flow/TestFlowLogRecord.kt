package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import org.springframework.data.mongodb.core.mapping.Document

@Document
data class TestFlowLogRecord(
    override val log: FlowLog,
    val data: String
) : FlowLogRecord<TestFlowLogRecord>() {
    override fun withLog(log: FlowLog): FlowLogRecord<TestFlowLogRecord> = copy(log = log)
    override fun getKey(): String {
        return log.eventType
    }
}
