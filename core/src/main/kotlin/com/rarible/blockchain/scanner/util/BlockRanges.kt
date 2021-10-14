package com.rarible.blockchain.scanner.util

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow

object BlockRanges {
    fun getRanges(from: Long, to: Long, step: Int): Flow<LongRange> =
        (from..to).chunked(step) {
            LongRange(it.first(), it.last())
        }.asFlow()
}
