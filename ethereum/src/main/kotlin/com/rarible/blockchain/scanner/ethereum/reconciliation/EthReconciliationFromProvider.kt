package com.rarible.blockchain.scanner.ethereum.reconciliation

import com.rarible.blockchain.scanner.reconciliation.ReconciliationFromProvider
import org.springframework.stereotype.Component

@Component
class EthReconciliationFromProvider: ReconciliationFromProvider {
    override fun initialFrom(): Long = 1L
}
