package com.rarible.blockchain.scanner.test.configuration

import com.rarible.core.test.ext.KafkaTest
import com.rarible.core.test.ext.MongoCleanup
import com.rarible.core.test.ext.MongoTest
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration

@MongoTest
@MongoCleanup
@ContextConfiguration(classes = [IntegrationTestConfiguration::class])
@SpringBootTest(
    properties = [
        "application.environment = test",
        "logging.logstash.tcp-socket.enabled = false",
        "logging.logjson.enabled = false",
    ]
)
@ActiveProfiles("test")
@KafkaTest
annotation class IntegrationTest
