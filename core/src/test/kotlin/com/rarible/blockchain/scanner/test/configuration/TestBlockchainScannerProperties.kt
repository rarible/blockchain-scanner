package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.core.daemon.DaemonWorkerProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.test")
data class TestBlockchainScannerProperties(
    override val retryPolicy: RetryPolicyProperties = RetryPolicyProperties(),
    override val scan: ScanProperties = ScanProperties(),
    override val monitoring: MonitoringProperties = MonitoringProperties(),
    override val daemon: DaemonWorkerProperties = DaemonWorkerProperties()
) : BlockchainScannerProperties {

    override val service: String = "test"
    override val blockchain: String = "test"

}
