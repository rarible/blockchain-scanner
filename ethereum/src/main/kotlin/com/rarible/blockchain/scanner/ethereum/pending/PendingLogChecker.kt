package com.rarible.blockchain.scanner.ethereum.pending

interface PendingLogChecker {

    suspend fun checkPendingLogs()

}