package com.rarible.blockchain.scanner.ethereum.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.core.daemon.DaemonWorkerProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import java.time.Duration

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.ethereum")
data class EthereumScannerProperties(

    val optimisticLockRetries: Long = 3,
    override val blockchain: String = "ethereum",
    override val service: String = "log-scanner",
    override val retryPolicy: RetryPolicyProperties,
    override val monitoring: MonitoringProperties,
    override val scan: ScanProperties = ScanProperties(),
    override val daemon: DaemonWorkerProperties = DaemonWorkerProperties(),
    val maxPendingLogDuration: Long = Duration.ofHours(2).toMillis()
) : BlockchainScannerProperties
