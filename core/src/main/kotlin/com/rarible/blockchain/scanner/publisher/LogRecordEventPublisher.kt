package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.model.LogRecord

interface LogRecordEventPublisher : RecordEventPublisher<LogRecord, LogRecordEvent>
