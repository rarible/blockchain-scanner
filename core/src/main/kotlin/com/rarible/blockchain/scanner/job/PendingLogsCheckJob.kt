package com.rarible.blockchain.scanner.job

import com.rarible.blockchain.scanner.pending.PendingLogChecker
import org.apache.commons.lang3.time.DateUtils
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
class PendingLogsCheckJob(
    private val PendingLogChecker: PendingLogChecker
) {

    @Scheduled(
        fixedRateString = "\${pendingLogsCheckJobInterval:${DateUtils.MILLIS_PER_HOUR}}",
        initialDelay = DateUtils.MILLIS_PER_MINUTE
    )
    fun job() {
        PendingLogChecker.checkPendingLogs()
    }
}