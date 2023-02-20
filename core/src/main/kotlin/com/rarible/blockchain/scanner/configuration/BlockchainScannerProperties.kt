package com.rarible.blockchain.scanner.configuration

import com.rarible.core.daemon.DaemonWorkerProperties

interface BlockchainScannerProperties {

    val blockchain: String

    val service: String

    val retryPolicy: RetryPolicyProperties
    val scan: ScanProperties
    val monitoring: MonitoringProperties
    val daemon: DaemonWorkerProperties
    val task: TaskProperties
}
