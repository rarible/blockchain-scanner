package com.rarible.blockchain.scanner.framework.util

import com.rarible.core.common.EventTimeMarks
import java.time.Instant

private const val stage = "scanner"

fun EventTimeMarks.addIn(date: Instant? = null) = this.addIn(stage, null, date)
fun EventTimeMarks.addOut(date: Instant? = null) = this.addOut(stage, null, date)

fun scannerBlockchainEventMarks(sourceDate: Instant? = null) = EventTimeMarks("blockchain")
    .add("source", sourceDate)
    .addIn()


