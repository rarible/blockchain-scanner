package com.rarible.blockchain.scanner.util

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.data.BlockEvent
import com.rarible.blockchain.scanner.handler.BlocksRange

object BlockRanges {

    fun getStableUnstableBlockRanges(
        baseBlockNumber: Long,
        lastBlockNumber: Long,
        batchSize: Int,
        stableDistance: Int
    ): Sequence<BlocksRange> {
        if (baseBlockNumber >= lastBlockNumber) {
            return emptySequence()
        }
        val fromId = baseBlockNumber + 1
        if (fromId + stableDistance > lastBlockNumber) {
            // All blocks are unstable.
            return getRanges(
                from = fromId,
                to = lastBlockNumber,
                step = batchSize
            ).map { BlocksRange(it, false) }
        }
        val stableId = lastBlockNumber - stableDistance
        val stableBlocks = getRanges(
            from = fromId,
            to = stableId,
            step = batchSize
        ).map { BlocksRange(it, true) }

        val unstableBlocks = getRanges(
            from = stableId + 1,
            to = lastBlockNumber,
            step = batchSize
        ).map { BlocksRange(it, false) }

        return stableBlocks + unstableBlocks
    }

    /**
     * Chunks consequent block events by type.
     */
    fun <BB : BlockchainBlock> toBatches(events: List<BlockEvent<BB>>): List<List<BlockEvent<BB>>> {
        val batches = mutableListOf<List<BlockEvent<BB>>>()
        val iterator = events.iterator()
        var current = iterator.next()
        var currentBatch = mutableListOf(current)

        while (iterator.hasNext()) {
            val next = iterator.next()
            if (next.javaClass == current.javaClass) {
                currentBatch += next
            } else {
                batches += currentBatch
                currentBatch = mutableListOf(next)
            }
            current = next
        }
        batches += currentBatch
        return batches
    }

    fun getRanges(from: Long, to: Long, step: Int): Sequence<LongRange> {
        check(from >= 0) { "$from "}
        check(to >= 0) { "$to" }
        if (from > to) return emptySequence()
        if (from == to) return sequenceOf(LongRange(from, to))
        return (from..to).asSequence().chunked(step) {
            LongRange(it.first(), it.last())
        }
    }

    fun toRanges(blockNumbers: List<Long>): List<LongRange> {
        if (blockNumbers.isEmpty()) {
            return emptyList()
        }
        val result = mutableListOf<LongRange>()

        val iter = blockNumbers.iterator()
        var start = iter.next()
        var current = start

        while (iter.hasNext()) {
            val next = iter.next()
            if (next != current + 1) {
                result += LongRange(start, current)
                start = next
            }
            current = next
        }

        result += LongRange(start, current)
        return result
    }
}
