package com.rarible.blockchain.scanner.configuration

data class JobProperties(
    val pendingLogs: PendingLogsJobProperties = PendingLogsJobProperties(),
    val reconciliation: ReconciliationJobProperties = ReconciliationJobProperties()
)