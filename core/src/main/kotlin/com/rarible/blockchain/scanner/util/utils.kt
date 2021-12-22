package com.rarible.blockchain.scanner.util

fun getBlockTopic(environment: String, service: String, blockchain: String): String {
    return "protocol.$environment.$blockchain.blockchain-scanner.$service.block"
}

fun getLogTopicPrefix(environment: String, service: String, blockchain: String): String {
    return "protocol.$environment.$blockchain.blockchain-scanner.$service.log"
}
