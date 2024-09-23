package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.block.BlockRepository
import com.rarible.blockchain.scanner.reconciliation.ReconciliationStateRepository
import org.springframework.context.annotation.ComponentScan

@Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@ComponentScan(
    basePackageClasses = [
        BlockRepository::class,
        ReconciliationStateRepository::class,
    ]
)
annotation class EnableBlockchainScannerComponents
