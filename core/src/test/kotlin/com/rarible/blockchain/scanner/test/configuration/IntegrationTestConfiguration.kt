package com.rarible.blockchain.scanner.test.configuration

import com.rarible.core.mongo.configuration.EnableRaribleMongo
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Configuration

@Configuration
@EnableAutoConfiguration
@EnableRaribleMongo
class IntegrationTestConfiguration {
    // This is empty because we test the scanner with unit testing, but use Mongo
}
