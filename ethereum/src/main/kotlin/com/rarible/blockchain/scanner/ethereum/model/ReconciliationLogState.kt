package com.rarible.blockchain.scanner.ethereum.model

import org.springframework.data.annotation.AccessType
import org.springframework.data.annotation.Id

data class ReconciliationLogState(
    val lastReconciledBlock: Long
) {
    @get:Id
    @get:AccessType(AccessType.Type.PROPERTY)
    val id: String = RECONCILIATION_LOG_STATE_ID

    companion object {
        const val RECONCILIATION_LOG_STATE_ID = "reconciliation_log_state"
    }
}