package com.rarible.blockchain.scanner.flow.configuration

import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.FlowChainId
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import com.rarible.blockchain.scanner.configuration.RetryPolicyProperties
import com.rarible.blockchain.scanner.configuration.ScanProperties
import com.rarible.blockchain.scanner.configuration.TaskProperties
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
    val chainId: FlowChainId = Flow.DEFAULT_CHAIN_ID,
    val proxy: String? = null,
): BlockchainScannerProperties