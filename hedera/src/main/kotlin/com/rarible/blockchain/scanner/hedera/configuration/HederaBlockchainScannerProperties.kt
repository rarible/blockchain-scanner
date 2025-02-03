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
import org.springframework.util.unit.DataSize
import java.time.Duration

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.hedera")
data class HederaBlockchainScannerProperties(
    override val blockchain: String = "hedera",
    override val service: String = "transaction-scanner",
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
    override val scan: ScanProperties = ScanProperties(),

    val mirrorNode: MirrorNodeClientProperties = MirrorNodeClientProperties(),
) : BlockchainScannerProperties

data class MirrorNodeClientProperties(
    val endpoint: String = "",
    val proxyUrl: String? = null,
    val mirrorNodeHeaders: Map<String, String> = emptyMap(),
    val timeout: Duration = Duration.ofSeconds(30),
    val maxBodySize: DataSize = DataSize.ofMegabytes(20),
    val maxErrorBodyLogLength: Int = 10000,
    val slowRequestLatency: Duration = Duration.ofSeconds(2),
)
