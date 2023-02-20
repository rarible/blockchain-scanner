package com.rarible.blockchain.scanner.flow.model

import com.rarible.blockchain.scanner.framework.model.LogRecord

abstract class FlowLogRecord : LogRecord {
    abstract val log: FlowLog
    override fun getBlock() = log.blockHeight
}
