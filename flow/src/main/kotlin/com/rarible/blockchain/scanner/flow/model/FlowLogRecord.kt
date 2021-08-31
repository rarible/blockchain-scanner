package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.LogRecord
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.FieldType
import org.springframework.data.mongodb.core.mapping.MongoId

@Document
data class FlowLogRecord(override val log: FlowLog): LogRecord<FlowLog, FlowLogRecord> {

    @get:MongoId(FieldType.STRING)
    val id: String
    get() = "${log.transactionHash}:${log.eventIndex}"

    override fun withLog(log: FlowLog): FlowLogRecord = copy(log = log)
}
