package com.rarible.blockchain.scanner.handler

data class TypedBlockRange(
    val range: LongRange,
    val stable: Boolean
) {

    override fun toString(): String {
        val stableWord = if (stable) "stable" else "unstable"
        if (range.first == range.last) {
            return "$stableWord block #${range.first}"
        }
        return "$stableWord blocks $range"
    }
}
