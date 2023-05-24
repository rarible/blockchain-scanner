package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.FlowChainId
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.service.SporkService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CachedSporksFlowGrpcApiTest {
    private val properties = FlowBlockchainScannerProperties(chainId = FlowChainId.MAINNET)
    private val sporkService = SporkService(properties)
    private val cachedSporksFlowGrpcApi = CachedSporksFlowGrpcApi(sporkService, properties)
    private val sporksFlowGrpcApi = SporksFlowGrpcApi(sporkService)

    @Test
    fun `get events by range - ok`() = runBlocking<Unit> {
        val blockBatchSize = 1
        val end = 53162294L
        val start = end - blockBatchSize
        val type = "A.e5bf4d436ca23932.BBxBarbiePack.Mint"

        val range = LongRange(start, end)
        val events = cachedSporksFlowGrpcApi.eventsByBlockRange(type, range).toList()
        val expectedEvents = sporksFlowGrpcApi.eventsByBlockRange(type, range).toList()
        assertThat(events).containsExactlyInAnyOrderElementsOf(expectedEvents)
    }

    @Test
    fun `get events by range - test`() = runBlocking<Unit> {
        val blockBatchSize = 0
        val end = 53162294L
        val start = end - blockBatchSize
        val type = "A.e5bf4d436ca23932.BBxBarbiePack.Mint"

        val range = LongRange(start, end)
        coroutineScope {
            (1..10).map {
                async {
                    cachedSporksFlowGrpcApi.eventsByBlockRange(type, range).toList()
                }
            }.awaitAll()
        }
    }
}