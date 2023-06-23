package com.rarible.blockchain.scanner.util

fun getLogTopicPrefix(environment: String, service: String, blockchain: String, type: String): String {
    return "protocol.$environment.$blockchain.blockchain-scanner.$service.$type"
}
