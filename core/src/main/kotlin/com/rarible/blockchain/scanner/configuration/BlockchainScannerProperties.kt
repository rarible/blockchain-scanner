package com.rarible.blockchain.scanner.configuration

import com.rarible.core.daemon.DaemonWorkerProperties

interface BlockchainScannerProperties {

    val blockchain: String
    val service: String
    val retryPolicy: RetryPolicyProperties
    val job: JobProperties
    val monitoring: MonitoringProperties
    val blockBufferSize: Int // TODO use it
    val daemon: DaemonWorkerProperties

}
