package com.rarible.blockchain.scanner.ethereum

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerConfiguration
import org.springframework.context.annotation.Import

@Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@Import(EthereumScannerConfiguration::class)
annotation class EnableEthereumBlockchainScanner
