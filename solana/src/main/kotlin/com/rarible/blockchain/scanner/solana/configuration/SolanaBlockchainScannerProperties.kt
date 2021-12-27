package com.rarible.blockchain.scanner.solana.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.core.daemon.DaemonWorkerProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import java.time.Duration

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.solana")
data class SolanaBlockchainScannerProperties(
    override val blockchain: String = "solana",
    override val service: String = "log-scanner",
    override val retryPolicy: RetryPolicyProperties = RetryPolicyProperties(),
    override val monitoring: MonitoringProperties = MonitoringProperties(
        worker = DaemonWorkerProperties(
            pollingPeriod = Duration.ofMillis(200L),
            errorDelay = Duration.ofSeconds(1L),
            backpressureSize = 100,
            buffer = true
        )
    ),
    override val daemon: DaemonWorkerProperties = DaemonWorkerProperties(),
    val rpcApiUrl: String,
    override val scan: ScanProperties = ScanProperties()
): BlockchainScannerProperties
