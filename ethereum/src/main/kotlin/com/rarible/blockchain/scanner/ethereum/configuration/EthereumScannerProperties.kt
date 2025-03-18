package com.rarible.blockchain.scanner.ethereum.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.ReconciliationProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.configuration.TaskProperties
import com.rarible.core.daemon.DaemonWorkerProperties
import org.springframework.boot.autoconfigure.cache.CacheProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.boot.context.properties.NestedConfigurationProperty
import java.net.URI
import java.time.Duration

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.ethereum")
data class EthereumScannerProperties(

    val optimisticLockRetries: Long = 3,
    override val blockchain: String = "ethereum",
    override val service: String = "log-scanner",
    override val retryPolicy: RetryPolicyProperties = RetryPolicyProperties(),
    override val monitoring: MonitoringProperties = MonitoringProperties(),
    override val scan: ScanProperties = ScanProperties(),
    override val daemon: DaemonWorkerProperties = DaemonWorkerProperties(),
    override val task: TaskProperties = TaskProperties(),
    override val reconciliation: ReconciliationProperties = ReconciliationProperties(
        enabled = true,
        autoReindexMode = ReconciliationProperties.ReindexMode.WITHOUT_EVENTS,
    ),
    val maxPendingLogDuration: Long = Duration.ofHours(2).toMillis(),
    val blockPoller: BlockPollerProperties = BlockPollerProperties(),
    val maxBatches: List<String> = emptyList(),
    val enableEthereumMonitor: Boolean = true,
    val logSaveBatchSize: Int = 300,
    val enableUnstableBlockParallelLoad: Boolean = false,
    val ignoreNullableLogs: Boolean = false,
    val ignoreEpochBlocks: Boolean = false,
    val ignoreLogWithoutTransaction: Boolean = false,
    @NestedConfigurationProperty
    val hyper: HyperProperties = HyperProperties()
) : BlockchainScannerProperties

data class HyperProperties(
    val enabled: Boolean = false,
    @NestedConfigurationProperty
    val s3: S3Properties = S3Properties(),
    @NestedConfigurationProperty
    val cache: CacheProperties = CacheProperties(),
) {
    data class S3Properties(
        val uri: URI = URI.create("s3://hl-mainnet-evm-blocks"),
        val username: String = "",
        val password: String = "",
    )

    data class CacheProperties(
        val size: Long = 5000,
        val expireAfterWrite: Duration = Duration.ofMinutes(5),
    )
}
