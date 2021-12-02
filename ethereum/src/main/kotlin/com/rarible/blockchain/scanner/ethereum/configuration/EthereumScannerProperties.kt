package com.rarible.blockchain.scanner.ethereum.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.core.daemon.DaemonWorkerProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.ethereum")
data class EthereumScannerProperties(

    val optimisticLockRetries: Long = 3,
    override val service: String = "scanner",
    override val retryPolicy: RetryPolicyProperties,
    override val job: EthereumScannerJobProperties,
    override val monitoring: MonitoringProperties,
    override val blockBufferSize: Int = 10,
    override val daemon: DaemonWorkerProperties = DaemonWorkerProperties()

) : BlockchainScannerProperties {

    override val blockchain: String = "ethereum"

}
