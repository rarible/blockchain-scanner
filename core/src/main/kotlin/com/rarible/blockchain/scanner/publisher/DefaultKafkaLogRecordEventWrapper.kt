package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

class DefaultKafkaLogRecordEventWrapper : KafkaLogRecordEventWrapper<LogRecordEvent> {
    override val targetClass: Class<LogRecordEvent>
        get() = LogRecordEvent::class.java

    override fun wrap(logRecordEvent: LogRecordEvent): LogRecordEvent = logRecordEvent
}