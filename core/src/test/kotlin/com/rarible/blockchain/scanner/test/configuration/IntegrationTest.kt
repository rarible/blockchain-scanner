package com.rarible.blockchain.scanner.test.configuration

import com.rarible.core.test.ext.EthereumTest
import com.rarible.core.test.ext.MongoCleanup
import com.rarible.core.test.ext.MongoTest
import kotlinx.coroutines.FlowPreview
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration

@FlowPreview
@MongoTest
@MongoCleanup
@ContextConfiguration(classes = [TestScannerConfiguration::class])
@SpringBootTest(
    properties = [
        "application.environment = test",
        "spring.cloud.consul.config.enabled = false",
        "spring.cloud.service-registry.auto-registration.enabled = false",
        "spring.cloud.discovery.enabled = false",
        "logging.logstash.tcp-socket.enabled = false"
    ]
)
@ActiveProfiles("test")
@EthereumTest
annotation class IntegrationTest
