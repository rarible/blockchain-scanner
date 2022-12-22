package com.rarible.blockchain.scanner.ethereum.reconciliation

import com.rarible.blockchain.scanner.framework.data.LogRecordEvent

interface OnReconciliationListener {
    suspend fun onLogRecordEvent(groupId: String, logRecordEvents: List<LogRecordEvent>)
}