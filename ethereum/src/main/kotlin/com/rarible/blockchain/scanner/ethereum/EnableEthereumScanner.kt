package com.rarible.blockchain.scanner.ethereum

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerConfiguration
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import org.springframework.context.annotation.Import

@FlowPreview
@Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@ExperimentalCoroutinesApi
@Import(EthereumScannerConfiguration::class)
annotation class EnableEthereumScanner
