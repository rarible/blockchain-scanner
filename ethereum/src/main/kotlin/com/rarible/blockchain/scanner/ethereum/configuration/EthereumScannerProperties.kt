package com.rarible.blockchain.scanner.ethereum.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.JobProperties
import com.rarible.blockchain.scanner.configuration.MonitoringProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.ethereum")
data class EthereumScannerProperties(


    override val maxProcessTime: Long,
    val maxAttempts: Long,
    val minBackoff: Long,
    val optimisticLockRetries: Long = 3,
    override val batchSize: Long,
    override val reconnectDelay: Long,
    override val reconnectAttempts: Int = Int.MAX_VALUE,
    override val job: JobProperties,
    override val monitoring: MonitoringProperties

) : BlockchainScannerProperties {

    override val blockchain: String = "ethereum"

}