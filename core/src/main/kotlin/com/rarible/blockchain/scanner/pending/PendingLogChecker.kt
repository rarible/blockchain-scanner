package com.rarible.blockchain.scanner.pending

interface PendingLogChecker {

    suspend fun checkPendingLogs()

}