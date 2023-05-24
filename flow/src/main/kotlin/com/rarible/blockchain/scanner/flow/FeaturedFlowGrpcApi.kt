package com.rarible.blockchain.scanner.flow

import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Component

@Component
@Primary
class FeaturedFlowGrpcApi(
    private val sporksFlowGrpcApi: SporksFlowGrpcApi,
    private val cachedSporksFlowGrpcApi: CachedSporksFlowGrpcApi,
    private val properties: FlowBlockchainScannerProperties
) : FlowGrpcApi by (if (properties.cacheBlockEvents.enabled) cachedSporksFlowGrpcApi else sporksFlowGrpcApi)