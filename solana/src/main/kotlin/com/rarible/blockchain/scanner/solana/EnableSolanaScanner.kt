package com.rarible.blockchain.scanner.solana

import com.rarible.blockchain.scanner.solana.configuration.SolanaBlockchainScannerConfiguration
import org.springframework.context.annotation.Import

@Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@Import(SolanaBlockchainScannerConfiguration::class)
annotation class EnableSolanaScanner