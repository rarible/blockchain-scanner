package com.rarible.blockchain.scanner.configuration

data class JobProperties(
    val pendingBlocks: PendingBlocksJobProperties = PendingBlocksJobProperties(),
    val pendingLogs: PendingLogsJobProperties = PendingLogsJobProperties(),
    val reconciliation: ReconciliationJobProperties = ReconciliationJobProperties()
)