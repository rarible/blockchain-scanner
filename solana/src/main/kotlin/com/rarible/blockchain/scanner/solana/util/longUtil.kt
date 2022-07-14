package com.rarible.blockchain.scanner.solana.util

fun Long.toFixedLengthString(length: Int): String {
    val string = toString()
    check(string.length <= length) { "$string has length more than $length" }
    return string.padStart(length, '0')
}
