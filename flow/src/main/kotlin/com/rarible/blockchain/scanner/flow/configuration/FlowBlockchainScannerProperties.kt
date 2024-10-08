package com.rarible.blockchain.scanner.flow.configuration

import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.FlowChainId
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.ReconciliationProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.configuration.TaskProperties
import com.rarible.blockchain.scanner.flow.service.Spork
import com.rarible.core.daemon.DaemonWorkerProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import java.time.Duration

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.flow")
data class FlowBlockchainScannerProperties(
    override val blockchain: String = "flow",
    override val service: String = "log-scanner",
    override val retryPolicy: RetryPolicyProperties = RetryPolicyProperties(),
    override val monitoring: MonitoringProperties = MonitoringProperties(
        worker = DaemonWorkerProperties(
            pollingPeriod = Duration.ofMillis(200L),
            errorDelay = Duration.ofSeconds(1L),
        )
    ),
    override val daemon: DaemonWorkerProperties = DaemonWorkerProperties(),
    override val scan: ScanProperties = ScanProperties(),
    override val task: TaskProperties = TaskProperties(),
    override val reconciliation: ReconciliationProperties = ReconciliationProperties(),
    val poller: BlockPollerProperties = BlockPollerProperties(),
    val chainId: FlowChainId = Flow.DEFAULT_CHAIN_ID,
    val httpApiClient: HttpApiClientProperties = HttpApiClientProperties(),
    val cacheBlockEvents: CacheBlockEventsProperties = CacheBlockEventsProperties(),
    val enableHttpClient: Boolean = false,
    val proxy: String? = null,
    val enableUseUndocumentedMethods: Boolean = false,
    val timeout: Duration = Duration.ofSeconds(30),
    val chunkSize: Int = 25,
    val sporks: List<Spork> = listOf(
        Spork(
            from = 0,
            nodeUrl = "access.devnet.nodes.onflow.org"
        )
    ),
) : BlockchainScannerProperties
