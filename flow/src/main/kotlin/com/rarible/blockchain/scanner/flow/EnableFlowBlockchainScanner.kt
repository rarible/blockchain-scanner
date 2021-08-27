package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerConfiguration
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.ObsoleteCoroutinesApi
import org.springframework.context.annotation.Import

@FlowPreview
@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
@Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@Import(FlowBlockchainScannerConfiguration::class)
annotation class EnableFlowBlockchainScanner
