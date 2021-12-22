package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

interface LogRecordEventPublisher {

    /**
     * Publish LogRecordEvents merged for the descriptor group and sorted using the blockchain-specific comparator.
     */
    suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent<*>>)

}
