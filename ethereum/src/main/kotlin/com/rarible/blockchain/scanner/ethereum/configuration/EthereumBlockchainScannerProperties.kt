package com.rarible.blockchain.scanner.ethereum.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.ethereum")
data class EthereumBlockchainScannerProperties(

    override val maxProcessTime: Long,
    val maxAttempts: Long,
    val minBackoff: Long,
    override val batchSize: Long,
    override val reconnectDelay: Long,
    override val reconnectAttempts: Int = Int.MAX_VALUE,
    override val reindexEnabled: Boolean

) : BlockchainScannerProperties