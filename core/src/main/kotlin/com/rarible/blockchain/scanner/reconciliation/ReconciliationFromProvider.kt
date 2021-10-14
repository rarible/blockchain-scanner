package com.rarible.blockchain.scanner.reconciliation

interface ReconciliationFromProvider {

    fun initialFrom(descriptorId: String): Long
}
