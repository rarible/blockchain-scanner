package com.rarible.blockchain.scanner.util

import kotlinx.coroutines.ExperimentalCoroutinesApi

@ExperimentalCoroutinesApi
suspend fun <T> logTime(label: String, f: suspend () -> T): T {
    val start = System.currentTimeMillis()
    return try {
        f()
    } finally {
        println("____ $label time: ${System.currentTimeMillis() - start}ms")
    }
}
