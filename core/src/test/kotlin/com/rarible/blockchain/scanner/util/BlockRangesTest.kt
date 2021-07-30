package com.rarible.blockchain.scanner.util

import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class BlockRangesTest {

    @Test
    fun ranges() = runBlocking {
        assertEquals(listOf(1L..9L), range(1, 9, 10))
        assertEquals(listOf(1L..10L), range(1, 10, 10))
        assertEquals(listOf(1L..10L, 11L..11L), range(1, 11, 10))
    }

    private suspend fun range(from: Long, to: Long, step: Long): List<LongRange> {
        return BlockRanges.getRanges(from, to, step).toCollection(mutableListOf())
    }

}