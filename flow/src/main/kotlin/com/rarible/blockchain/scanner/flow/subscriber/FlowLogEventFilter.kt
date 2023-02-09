package com.rarible.blockchain.scanner.flow.subscriber

import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.framework.subscriber.LogEventFilter

interface FlowLogEventFilter : LogEventFilter<FlowLogRecord, FlowDescriptor>