package com.rarible.blockchain.scanner.configuration

interface BlockchainScannerProperties {

    val blockchain: String
    val maxProcessTime: Long
    val batchSize: Long
    val reconnectDelay: Long
    val reconnectAttempts: Int
    val job: JobProperties
    val monitoring: MonitoringProperties

}