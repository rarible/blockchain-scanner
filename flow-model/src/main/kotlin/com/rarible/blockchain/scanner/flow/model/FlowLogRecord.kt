package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.LogRecord

abstract class FlowLogRecord<LR: FlowLogRecord<LR>>: LogRecord<FlowLog, FlowLogRecord<LR>> {
    abstract override val log: FlowLog
}
