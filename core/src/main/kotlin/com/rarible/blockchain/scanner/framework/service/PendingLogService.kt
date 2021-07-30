package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.data.LogEvent
import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.model.Log
import kotlinx.coroutines.flow.Flow

interface PendingLogService<BB, L : Log> {

    fun markInactive(block: BB, logs: List<LogEvent<L>>): Flow<LogEventStatusUpdate<L>>

}