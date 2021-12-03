package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.model.LogRecord

interface LogEventPublisher {

    suspend fun publish(logs: List<LogRecord<*, *>>)

}