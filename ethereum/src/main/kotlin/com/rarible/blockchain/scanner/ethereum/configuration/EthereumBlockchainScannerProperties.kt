package com.rarible.blockchain.scanner.ethereum.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "blockchain.scanner.ethereum")
class EthereumBlockchainScannerProperties(

    override val maxProcessTime: Long,
    val maxAttempts: Long,
    val minBackoff: Long,
    override val batchSize: Long,
    override val reconnectDelay: Long,
    override val reindexEnabled: Boolean

) : BlockchainScannerProperties