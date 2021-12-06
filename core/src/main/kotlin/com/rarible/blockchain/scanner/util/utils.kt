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

fun getBlockTopic(environment: String, service: String, blockchain: String): String {
    return "protocol.$environment.$blockchain.blockchain-scanner.$service.block"
}

fun getLogTopicPrefix(environment: String, service: String, blockchain: String): String {
    return "protocol.$environment.$blockchain.blockchain-scanner.$service.log"
}

