package com.rarible.blockchain.scanner.ethereum.test

import com.rarible.core.test.ext.EthereumTest
import com.rarible.core.test.ext.KafkaCleanup
import com.rarible.core.test.ext.KafkaTest
import com.rarible.core.test.ext.MongoCleanup
import com.rarible.core.test.ext.MongoTest
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration

@MongoTest
@MongoCleanup
@ContextConfiguration(classes = [TestEthereumScannerConfiguration::class])
@SpringBootTest(
    properties = [
        "application.environment = test",
        "logging.logstash.tcp-socket.enabled = false",
        "logging.logjson.enabled = false",
    ]
)
@ActiveProfiles("test")
@EthereumTest
@KafkaTest
@KafkaCleanup
annotation class IntegrationTest
