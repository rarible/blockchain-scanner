package com.rarible.blockchain.scanner.configuration

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
@Qualifier("blockchainScannerPropertiesProvider")
class BlockchainScannerPropertiesProvider(
    val properties: BlockchainScannerProperties
) {

    private val logger = LoggerFactory.getLogger(BlockchainScannerPropertiesProvider::class.java)

    @PostConstruct
    fun init() {
        logger.info("Starting blockchain scanner with next configuration: {}", properties)
    }
}
