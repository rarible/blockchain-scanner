package com.rarible.blockchain.scanner.hedera.utils

import java.time.Instant

fun consensusTimestampToInstant(timestamp: String): Instant {
    val parts = timestamp.split(".")
    if (parts.size != 2) throw IllegalArgumentException("Invalid consensus timestamp format: $timestamp")
    val seconds = parts[0].toLong()
    val nanoSeconds = parts[1].toLong()
    return Instant.ofEpochSecond(seconds).plusNanos(nanoSeconds)
}
