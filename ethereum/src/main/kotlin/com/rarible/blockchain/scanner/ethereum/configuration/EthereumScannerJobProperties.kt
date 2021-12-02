package com.rarible.blockchain.scanner.ethereum.configuration

import com.rarible.blockchain.scanner.configuration.JobProperties
import com.rarible.blockchain.scanner.configuration.ReconciliationJobProperties

class EthereumScannerJobProperties(
    reconciliation: ReconciliationJobProperties,
    val pendingLogs: EthereumPendingLogsJobProperties = EthereumPendingLogsJobProperties()
) : JobProperties(reconciliation)