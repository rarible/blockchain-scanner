package com.rarible.blockchain.scanner.hedera

import com.rarible.blockchain.scanner.hedera.configuration.HederaBlockchainScannerConfiguration
import org.springframework.context.annotation.Import

@Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@Import(HederaBlockchainScannerConfiguration::class)
annotation class EnableHederaScanner
