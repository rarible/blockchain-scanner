package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

interface LogEventListener<L : Log> {

    suspend fun onBlockLogsProcessed(blockEvent: ProcessedBlockEvent<L>)

    suspend fun onPendingLogsDropped(logs: List<LogRecord<L>>)

}