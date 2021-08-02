package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.test")
class TestBlockchainScannerProperties(

    override val maxProcessTime: Long,
    override val batchSize: Long,
    override val reconnectDelay: Long,
    override val reindexEnabled: Boolean

) : BlockchainScannerProperties