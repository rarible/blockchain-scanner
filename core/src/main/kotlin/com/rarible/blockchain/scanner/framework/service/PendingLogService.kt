package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.data.LogEvent
import com.rarible.blockchain.scanner.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import kotlinx.coroutines.flow.Flow

interface PendingLogService<BB, L : Log, D : Descriptor> {

    /**
     * Finds log events to mark inactive
     * @param block block received
     * @param logs list of pending logs currently found
     * @return LogEventStatusUpdate's - what log events need to change status
     */
    fun markInactive(block: BB, logs: List<LogEvent<L, D>>): Flow<LogEventStatusUpdate<L, D>>

}