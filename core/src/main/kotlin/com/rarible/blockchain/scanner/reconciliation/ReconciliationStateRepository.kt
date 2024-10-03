package com.rarible.blockchain.scanner.reconciliation

import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.data.mongodb.core.ReactiveMongoOperations
import org.springframework.stereotype.Component

@Component
class ReconciliationStateRepository(
    private val mongo: ReactiveMongoOperations
) {
    suspend fun saveReconciliationLogState(state: ReconciliationLogState): ReconciliationLogState {
        return mongo.save(state, COLLECTION).awaitFirst()
    }

    suspend fun getReconciliationLogState(): ReconciliationLogState? {
        return mongo.findById(
            ReconciliationLogState.RECONCILIATION_LOG_STATE_ID,
            ReconciliationLogState::class.java,
            COLLECTION
        ).awaitFirstOrNull()
    }

    private companion object {
        const val COLLECTION = "blockchain_scanner_reconciliation"
    }
}
