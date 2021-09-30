package com.rarible.blockchain.scanner.pending

import java.time.Duration

interface PendingBlockChecker {

    suspend fun checkPendingBlocks(pendingBlockAgeToCheck: Duration)

}