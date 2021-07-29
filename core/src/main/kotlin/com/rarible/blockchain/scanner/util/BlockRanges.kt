package com.rarible.blockchain.scanner.util

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map

object BlockRanges {
    fun getRanges(from: Long, to: Long, step: Long): Flow<LongRange> {
        return (from..to).step(step).asFlow()
            .map { start ->
                start..minOf(start + step - 1, to)
            }
    }
}