package com.rarible.blockchain.scanner.configuration

interface BlockchainScannerProperties {

    val maxProcessTime: Long
    val batchSize: Long
    val reconnectDelay: Long
    val reconnectAttempts: Int
    val job: JobProperties

}