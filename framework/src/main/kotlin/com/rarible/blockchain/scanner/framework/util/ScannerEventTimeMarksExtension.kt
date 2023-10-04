package com.rarible.blockchain.scanner.framework.util

import com.rarible.blockchain.scanner.framework.data.ScanMode
import com.rarible.core.common.EventTimeMarks
import java.time.Instant

private const val scannerStage = "scanner"

private const val sourceStage = "source"

fun EventTimeMarks.addScannerIn(date: Instant? = null) = this.addIn(scannerStage, null, date)

fun EventTimeMarks.addScannerOut(date: Instant? = null) = this.addOut(scannerStage, null, date)

fun scannerBlockchainEventMarks(
    mode: ScanMode,
    sourceDate: Instant? = null,
    sourceOutDate: Instant? = null
): EventTimeMarks {
    return EventTimeMarks(mode.eventSource)
        .add(sourceStage, sourceDate)
        .addOut(sourceStage, null, sourceOutDate)
        .addScannerIn()
}
