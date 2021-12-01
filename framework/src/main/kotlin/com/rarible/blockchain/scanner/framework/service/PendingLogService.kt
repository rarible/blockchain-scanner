package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.data.LogEvent
import com.rarible.blockchain.scanner.framework.data.LogEventStatusUpdate
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import kotlinx.coroutines.flow.Flow

interface PendingLogService<L : Log<L>, R : LogRecord<L, *>, D : Descriptor> {

    /**
     * Finds log events to mark inactive
     * @param blockHash hash of received block
     * @param records list of pending logs currently found
     * @return LogEventStatusUpdate's - what log events need to change status
     */
    fun getInactive(blockHash: String, records: List<LogEvent<L, R, D>>): Flow<LogEventStatusUpdate<L, R, D>>

}
