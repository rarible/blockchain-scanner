package com.rarible.blockchain.scanner.flow.subscriber

import com.rarible.blockchain.scanner.flow.model.FlowLog
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.subscriber.LogEventListener

interface FlowLogEventListener: LogEventListener<FlowLog, FlowLogRecord<*>>
