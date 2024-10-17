package com.rarible.blockchain.scanner.configuration

import java.time.Duration

data class ReconciliationProperties(
    val enabled: Boolean = false,
    val blockHandleParallelism: Int = 50,
    val autoReindexMode: ReindexMode = ReindexMode.WITH_EVENTS,
    val reindexBatchSize: Int = 50,
    val reindexParallelism: Int = 10,
    val checkPeriod: Duration = Duration.ofMinutes(1),
    /**
     * Reconciliation job is running behind the process of indexing current blocks up to this amount
     * */
    val blockLag: Int = 32,
) {
    enum class ReindexMode(
        val enabled: Boolean,
        val publishEvents: Boolean,
    ) {
        DISABLED(enabled = false, publishEvents = false),
        /**
         * Must be used if the indexer performs full reduce task on a regular schedule.
         * There is no sense in doing incremental reduce by the events produced during reindex
         * because full reindex will be run anyway.
         * */
        WITHOUT_EVENTS(enabled = true, publishEvents = false),
        WITH_EVENTS(enabled = false, publishEvents = true),
    }
}
