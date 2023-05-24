package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.FlowChainId
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.service.SporkService
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class CachedSporksFlowGrpcApiTest {
    private val properties = FlowBlockchainScannerProperties(chainId = FlowChainId.MAINNET)
    private val sporkService = SporkService(properties)
    private val cachedSporksFlowGrpcApi = CachedSporksFlowGrpcApi(sporkService, properties)

    @Test
    fun `get events by reange - ok`() = runBlocking<Unit> {
        val blockBatchSize = 250L
        val end = 53162294L
        val start = end - blockBatchSize

        val range = LongRange(start, end)
        val events = cachedSporksFlowGrpcApi.eventsByBlockRange("A.e5bf4d436ca23932.BBxBarbiePack.Mint", range).toList()
        println(events)
    }
}