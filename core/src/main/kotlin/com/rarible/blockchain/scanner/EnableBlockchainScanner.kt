package com.rarible.blockchain.scanner

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ExperimentalCoroutinesApi
import com.rarible.blockchain.scanner.configuration.BlockchainScannerConfiguration
import org.springframework.context.annotation.Import

@FlowPreview
@Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@ExperimentalCoroutinesApi
@Import(BlockchainScannerConfiguration::class)
annotation class EnableBlockchainScanner
