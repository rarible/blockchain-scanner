package com.rarible.blockchain.scanner.reconciliation

interface ReconciliationFromProvider {

    fun initialFrom(groupId: String): Long
}
