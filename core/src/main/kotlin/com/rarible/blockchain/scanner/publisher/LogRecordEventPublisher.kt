package com.rarible.blockchain.scanner.publisher

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

interface LogRecordEventPublisher {

    /**
     * Optional hook to prepare the group for publishing new records.
     * This may include creation of a Kafka topic with the given number of partitions.
     */
    suspend fun prepareGroup(groupId: String): Unit = Unit

    /**
     * Determines is publisher enabled or not
     */
    suspend fun isEnabled(): Boolean = true

    /**
     * Publish LogRecordEvents merged for the descriptor group and sorted using the blockchain-specific comparator.
     */
    suspend fun publish(groupId: String, logRecordEvents: List<LogRecordEvent>)

}
