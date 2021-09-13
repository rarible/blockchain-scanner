package com.rarible.blockchain.scanner.configuration

interface BlockchainScannerProperties {

    val blockchain: String
    val retryPolicy: RetryPolicyProperties
    val job: JobProperties
    val monitoring: MonitoringProperties
    val blockBufferSize: Int
}
