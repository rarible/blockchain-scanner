package com.rarible.blockchain.scanner.flow.configuration

import com.rarible.blockchain.scanner.EnableBlockchainScanner
import com.rarible.blockchain.scanner.flow.FlowGrpcApi
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainClient
import com.rarible.blockchain.scanner.flow.client.FlowBlockchainClientFactory
import com.rarible.blockchain.scanner.flow.service.FlowApiFactory
import com.rarible.blockchain.scanner.flow.service.SporkService
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories

@EnableBlockchainScanner
@Configuration
@ComponentScan(basePackages = ["com.rarible.blockchain.scanner.flow"])
@EnableReactiveMongoRepositories(basePackages = ["com.rarible.blockchain.scanner.flow.repository"])
@EnableConfigurationProperties(FlowBlockchainScannerProperties::class)
class FlowBlockchainScannerConfiguration {

    @Bean
    fun grpcApi(blockchainClientFactory: FlowBlockchainClientFactory): FlowGrpcApi = blockchainClientFactory.flowGrpcApi

    @Bean
    fun blockchainClient(blockchainClientFactory: FlowBlockchainClientFactory): FlowBlockchainClient =
        blockchainClientFactory.createMainClient()

    @Bean
    fun flowApiFactory(blockchainClientFactory: FlowBlockchainClientFactory): FlowApiFactory =
        blockchainClientFactory.apiFactory

    @Bean
    fun sporkService(blockchainClientFactory: FlowBlockchainClientFactory): SporkService =
        blockchainClientFactory.sporkService
}
