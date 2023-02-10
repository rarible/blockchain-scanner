package com.rarible.blockchain.scanner.flow.client

import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.bytesToHex
import com.nftco.flow.sdk.impl.AsyncFlowAccessApiImpl
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
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
    private val archiveApi = run {
        val node = URI.create("https://access-001.mainnet15.nodes.onflow.org:9000")
        logger.info("host=${node.host}, port=${node.port}")

        val channel = ManagedChannelBuilder.forAddress(node.host, node.port)
            .maxInboundMessageSize(33554432)
            .usePlaintext()
            .userAgent(Flow.DEFAULT_USER_AGENT)
            .build()
        AsyncFlowAccessApiImpl(AccessAPIGrpc.newFutureStub(channel))
        AsyncFlowAccessApiImpl(AccessAPIGrpc.newFutureStub(channel))
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
        val block = archiveApi.getBlockByHeight(22156274).await()
        logger.info(
            buildString {
                append("\n")
                append("height=${block?.height}\n")
                append("hash=${block?.id?.bytes?.bytesToHex()}\n")
            }
        )
    }

    private companion object {
        val logger: Logger = LoggerFactory.getLogger(FlowBlockchainClientTest::class.java)
    }

}