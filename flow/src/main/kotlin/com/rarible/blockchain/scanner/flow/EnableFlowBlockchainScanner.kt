package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerConfiguration
import org.springframework.context.annotation.Import

@Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@Import(FlowBlockchainScannerConfiguration::class)
annotation class EnableFlowBlockchainScanner
