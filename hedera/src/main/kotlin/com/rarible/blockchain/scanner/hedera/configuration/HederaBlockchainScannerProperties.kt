package com.rarible.blockchain.scanner.hedera.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.ReconciliationProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.configuration.TaskProperties
import com.rarible.core.daemon.DaemonWorkerProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import java.time.Duration

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.hedera")
data class HederaBlockchainScannerProperties(
    override val blockchain: String = "hedera",
    override val service: String = "log-scanner",
    override val retryPolicy: RetryPolicyProperties = RetryPolicyProperties(),
    override val monitoring: MonitoringProperties = MonitoringProperties(
        worker = DaemonWorkerProperties(
            pollingPeriod = Duration.ofMillis(200L),
            errorDelay = Duration.ofSeconds(1L)
        )
    ),
    override val daemon: DaemonWorkerProperties = DaemonWorkerProperties(),
    override val task: TaskProperties = TaskProperties(),
    override val reconciliation: ReconciliationProperties = ReconciliationProperties(),
    val nodes: List<String>,
    val reconciliationNodes: List<String> = nodes,
    val apiTimeout: Long = 30000,
    val haEnabled: Boolean = false,
    val monitoringThreadInterval: Duration = Duration.ofSeconds(30),
    val maxBlockDelay: Duration = Duration.ofMinutes(10),
    override val scan: ScanProperties = ScanProperties()
) : BlockchainScannerProperties
