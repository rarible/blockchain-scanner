package com.rarible.blockchain.scanner.subscriber

import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord

/**
 * External listeners of all LogEvents. Any change of any log will be posted to
 * such listener.
 */
interface LogEventListener<L : Log, R : LogRecord<L, *>> {

    /**
     * Triggered when entire block processed. Changes of for all logs of the block will be published here.
     * The only exception here is reindex operation - reindex publishing log changes per block for specific
     * subscriber. It means, if there are several subscribers, this method will be triggered multiple times -
     * once for each subscriber. And pack of logs will contain only log events specific for the subscriber.
     */
    suspend fun onBlockLogsProcessed(blockEvent: ProcessedBlockEvent<L, R>)

    /**
     * Method called when background job checks pending logs and find some of them should be dropped.
     * Such packs of logs are not related to specific block or subscriber and could contain logs from different blocks.
     */
    suspend fun onPendingLogsDropped(logs: List<R>)

}