package com.rarible.blockchain.scanner.solana.client.test

import com.rarible.core.test.ext.KafkaTest
import com.rarible.core.test.ext.MongoCleanup
import com.rarible.core.test.ext.MongoTest
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ContextConfiguration

@MongoTest
@MongoCleanup
@ContextConfiguration(classes = [TestSolanaScannerConfiguration::class])
@SpringBootTest(
    properties = [
        "application.environment = test",
        "spring.cloud.consul.config.enabled = false",
        "spring.cloud.service-registry.auto-registration.enabled = false",
        "spring.cloud.discovery.enabled = false",
        "logging.logstash.tcp-socket.enabled = false"
    ]
)
@KafkaTest
annotation class IntegrationTest
