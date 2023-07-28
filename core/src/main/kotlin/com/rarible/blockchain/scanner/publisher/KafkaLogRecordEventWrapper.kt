package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent
import com.rarible.blockchain.scanner.framework.model.LogRecord

interface KafkaLogRecordEventWrapper<E> : KafkaRecordEventWrapper<E, LogRecord, LogRecordEvent>
