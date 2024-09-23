package com.rarible.blockchain.scanner.reconciliation

import org.springframework.data.annotation.AccessType
import org.springframework.data.annotation.Id

data class ReconciliationLogState(
    val lastReconciledBlock: Long
) {
    @get:Id
    @get:AccessType(AccessType.Type.PROPERTY)
    var id: String
        get() = RECONCILIATION_LOG_STATE_ID
        set(_) {}

    companion object {
        const val RECONCILIATION_LOG_STATE_ID = "reconciliation_log_state"
    }
}
