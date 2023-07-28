package com.rarible.blockchain.scanner

import com.rarible.blockchain.scanner.block.BlockRepository
import org.springframework.context.annotation.ComponentScan

@Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@ComponentScan(
    basePackageClasses = [
        BlockRepository::class
    ]
)
annotation class EnableBlockchainScannerComponents
