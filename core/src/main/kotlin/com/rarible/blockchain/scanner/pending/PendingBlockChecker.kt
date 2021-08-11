package com.rarible.blockchain.scanner.pending

import java.time.Duration

interface PendingBlockChecker {

    fun checkPendingBlocks(pendingBlockAgeToCheck: Duration)

}