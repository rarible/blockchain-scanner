package com.rarible.blockchain.scanner.flow.service

data class Spork(
    val from: Long,
    val to: Long = Long.MAX_VALUE,
    val nodeUrl: String,
    val port: Int = 9000,
) {

    fun containsBlock(blockHeight: Long): Boolean = blockHeight in from..to

    fun trim(range: LongRange): LongRange {
        val first = if (from < range.first) range.first else from
        val last = if (to < range.last) to else range.last
        return first..last
    }
}
