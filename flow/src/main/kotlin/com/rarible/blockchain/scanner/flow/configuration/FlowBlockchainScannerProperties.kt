package com.rarible.blockchain.scanner.flow.configuration

import com.rarible.blockchain.scanner.configuration.*
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.flow")
data class FlowBlockchainScannerProperties(
    override val blockchain: String = "flow",
    override val retryPolicy: RetryPolicyProperties = RetryPolicyProperties(),
    override val monitoring: MonitoringProperties = MonitoringProperties(),
    override val job: JobProperties = JobProperties(reconciliation = ReconciliationJobProperties(batchSize = 1000L))
): BlockchainScannerProperties
