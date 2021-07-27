package com.rarible.blockchain.scanner.ethereum.configuration

import com.rarible.blockchain.scanner.configuration.BlockchainScannerConfiguration
import com.rarible.blockchain.scanner.ethereum.EthereumScanner
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import

@Configuration
@ComponentScan(basePackageClasses = [EthereumScanner::class])
@Import(BlockchainScannerConfiguration::class)
class EthereumScannerConfiguration