package com.rarible.blockchain.scanner.configuration

import com.rarible.blockchain.scanner.BlockchainScanner
import com.rarible.core.task.EnableRaribleTask
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling

@Configuration
@ComponentScan(basePackageClasses = [BlockchainScanner::class])
@EnableScheduling
@EnableRaribleTask
class BlockchainScannerConfiguration