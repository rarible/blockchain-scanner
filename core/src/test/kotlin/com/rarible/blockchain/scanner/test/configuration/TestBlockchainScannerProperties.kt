package com.rarible.blockchain.scanner.test.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.configuration.JobProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "blockchain.scanner.test")
data class TestBlockchainScannerProperties(

    override val maxProcessTime: Long,
    override val batchSize: Long,
    override val reconnectDelay: Long,
    override val reconnectAttempts: Int = Int.MAX_VALUE,
    override val job: JobProperties

) : BlockchainScannerProperties