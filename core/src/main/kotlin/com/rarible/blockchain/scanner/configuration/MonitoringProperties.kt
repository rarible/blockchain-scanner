package com.rarible.blockchain.scanner.configuration

import com.rarible.core.daemon.DaemonWorkerProperties

data class MonitoringProperties(
    val enabled: Boolean = true,
    val rootPath: String = "blockchain.scanner",
    val worker: DaemonWorkerProperties = DaemonWorkerProperties()
)