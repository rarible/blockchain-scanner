package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.bytesToHex
import com.nftco.flow.sdk.impl.AsyncFlowAccessApiImpl
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.http.FlowHttpClientImpl
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.onflow.protobuf.access.AccessAPIGrpc
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URI

class FlowBlockchainClientTest {
    private val api = run {
        val node = URI.create("https://access.mainnet.nodes.onflow.org:9000")
        logger.info("host=${node.host}, port=${node.port}")

        val channel = ManagedChannelBuilder.forAddress(node.host, node.port)
            .maxInboundMessageSize(33554432)
            .usePlaintext()
            .userAgent(Flow.DEFAULT_USER_AGENT)
            .build()
        AsyncFlowAccessApiImpl(AccessAPIGrpc.newFutureStub(channel))
    }
    private val testnetApi = run {
        val node = URI.create("https://access.devnet.nodes.onflow.org:9000")
        logger.info("host=${node.host}, port=${node.port}")

        val channel = ManagedChannelBuilder.forAddress(node.host, node.port)
            .maxInboundMessageSize(33554432)
            .usePlaintext()
            .userAgent(Flow.DEFAULT_USER_AGENT)
            .build()
        AsyncFlowAccessApiImpl(AccessAPIGrpc.newFutureStub(channel))
        AsyncFlowAccessApiImpl(AccessAPIGrpc.newFutureStub(channel))
    }

    private val httpClient = FlowHttpClientImpl(FlowBlockchainScannerProperties())

    @Test
    @Disabled
    fun `get events - ok`() = runBlocking<Unit> {
        val type = "A.e5bf4d436ca23932.BBxBarbiePack.Mint"
        val range = LongRange(52153800, 52153949)

        val httpEvents = httpClient
            .eventsByBlockRange(type, range).toList()
        val grpcEvents = api
            .getEventsForHeightRange(type, range).await()
            .filter { it.events.isNotEmpty() }

        Assertions.assertThat(httpEvents.size).isEqualTo(4)
        Assertions
            .assertThat(httpEvents)
            .isEqualTo(grpcEvents)
    }

    @Test
    @Disabled
    fun `get latest block`() = runBlocking<Unit> {
        val block = api.getLatestBlock(true)
        logger.info(
            buildString {
                append("\n")
                append("height=${block.get().height}\n")
                append("hash=${block.get().id.bytes.bytesToHex()}\n")
            }
        )
    }

    @Test
    @Disabled
    fun `get block by id`() = runBlocking<Unit> {
        val block = testnetApi.getBlockByHeight(22156274).await()
        logger.info(
            buildString {
                append("\n")
                append("height=${block?.height}\n")
                append("hash=${block?.id?.bytes?.bytesToHex()}\n")
            }
        )
    }

    @Test
    @Disabled
    fun `get event by type for card`() = runBlocking<Unit> {
        val eventType = "A.80102bce1de42dc4.HWGaragePM.UpdateTokenEditionMetadata"
        val range = LongRange(95812893, 95812893)
        val blockEvents = testnetApi.getEventsForHeightRange(eventType, range).await()
        blockEvents.forEach { result ->
            result.events.forEach { event ->
                val stringValue = String(event.payload.bytes)
                println(stringValue)
            }
        }

    }

    @Test
    @Disabled
    fun `get event by type for pack`() = runBlocking<Unit> {
        val eventType = "A.80102bce1de42dc4.HWGaragePM.UpdatePackEditionMetadata"
        val testEventType = "UpdatePackEditionMetadata"
        val range = LongRange(95823562, 95823562)
        val blockEvents = testnetApi.getEventsForHeightRange(testEventType, range).await()
        blockEvents.forEach { result ->
            result.events.forEach { event ->
                val stringValue = String(event.payload.bytes)
                println(stringValue)
            }
        }

    }

    @Test
    @Disabled
    fun `get event by type for sftV2 ListingAvailable`() = runBlocking<Unit> {
        val eventType = "A.4eb8a10cb9f87357.NFTStorefrontV2.ListingAvailable"
        val range = LongRange(47570142, 47570142)
        val blockEvents = api.getEventsForHeightRange(eventType, range).await()
        blockEvents.forEach { result ->
            result.events.forEach { event ->
                println("----------> event: ${event.payload.stringValue}")
            }
        }
    }

    @Test
    @Disabled
    fun `get event by type for sftV1 ListingAvailable`() = runBlocking<Unit> {
        val eventType = "A.4eb8a10cb9f87357.NFTStorefront.ListingAvailable"
        val range = LongRange(47582240, 47582240)
        val blockEvents = api.getEventsForHeightRange(eventType, range).await()
        blockEvents.forEach { result ->
            result.events.first().let { event ->
                println("----------> event: ${event.payload.stringValue}")
            }
        }
    }

    @Test
    @Disabled
    fun `get event by type for sftV2 ListingCompleted`() = runBlocking<Unit> {
        val eventType = "A.4eb8a10cb9f87357.NFTStorefrontV2.ListingCompleted"
        val range = LongRange(47570130, 47570130)
        val blockEvents = api.getEventsForHeightRange(eventType, range).await()
        blockEvents.forEach { result ->
            result.events.forEach { event ->
                println("----------> event: ${event.payload.stringValue}")
            }
        }
    }

    @Test
    @Disabled
    fun `get event by type for sftV1 ListingCompleted`() = runBlocking<Unit> {
        val eventType = "A.4eb8a10cb9f87357.NFTStorefront.ListingCompleted"
        val range = LongRange(47640243, 47640243)
        val blockEvents = api.getEventsForHeightRange(eventType, range).await()
        blockEvents.forEach { result ->
            result.events.forEach { event ->
                println("----------> event: ${event.payload.stringValue}")
            }
        }
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(FlowBlockchainClientTest::class.java)
    }

}