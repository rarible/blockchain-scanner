package com.rarible.blockchain.scanner.reconciliation

import com.rarible.blockchain.scanner.handler.TypedBlockRange

interface ReconciliationLogHandler {
    suspend fun check(blocksRange: TypedBlockRange, batchSize: Int): Long
}
