package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

interface LogEventListener<L : Log, R : LogRecord<L, *>> {

    suspend fun onBlockLogsProcessed(blockEvent: ProcessedBlockEvent<L, R>)

    suspend fun onPendingLogsDropped(logs: List<R>)

}