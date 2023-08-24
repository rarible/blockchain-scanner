package com.rarible.blockchain.scanner.flow.test

import com.rarible.core.mongo.configuration.EnableRaribleMongo
import com.rarible.core.test.ext.MongoCleanup
import com.rarible.core.test.ext.MongoTest
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration

@MongoTest
@MongoCleanup
@EnableRaribleMongo
@ContextConfiguration(classes = [TestFlowScannerConfiguration::class])
@SpringBootTest(
    properties = [
        "application.environment = test",
        "spring.cloud.consul.config.enabled = false",
        "spring.cloud.service-registry.auto-registration.enabled = false",
        "spring.cloud.discovery.enabled = false",
        "logging.logstash.tcp-socket.enabled = false",
        "logging.logjson.enabled = false",
        "logging.logjson.enabled = false",
    ]
)
@ActiveProfiles("test")
annotation class IntegrationTest
