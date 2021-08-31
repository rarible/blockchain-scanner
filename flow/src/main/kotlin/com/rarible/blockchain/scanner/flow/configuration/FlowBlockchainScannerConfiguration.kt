package com.rarible.blockchain.scanner.flow.configuration

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories

@ObsoleteCoroutinesApi
@ExperimentalCoroutinesApi
@FlowPreview
@Configuration
@ComponentScan(basePackages = ["com.rarible.blockchain.scanner.flow"])
@EnableReactiveMongoRepositories(basePackages = ["com.rarible.blockchain.scanner.flow.repository"])
@EnableConfigurationProperties(FlowBlockchainScannerProperties::class)
class FlowBlockchainScannerConfiguration
