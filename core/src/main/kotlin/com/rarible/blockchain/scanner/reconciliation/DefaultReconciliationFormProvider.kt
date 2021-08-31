package com.rarible.blockchain.scanner.reconciliation

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.stereotype.Component

@Component
@ConditionalOnMissingBean(ReconciliationFromProvider::class)
class DefaultReconciliationFormProvider: ReconciliationFromProvider {
    override fun initialFrom(): Long = 1L
}
