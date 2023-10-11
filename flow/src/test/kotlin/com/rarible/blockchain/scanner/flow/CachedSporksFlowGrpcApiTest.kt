package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.FlowBlock
import com.nftco.flow.sdk.FlowChainId
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.service.FlowApiFactoryImpl
import com.rarible.blockchain.scanner.flow.service.SESSION_HASH_HEADER
import com.rarible.blockchain.scanner.flow.service.SporkService
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import io.grpc.Metadata
import io.grpc.Server
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.StreamObserver
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.onflow.protobuf.access.Access
import org.onflow.protobuf.access.Access.BlockResponse
import org.onflow.protobuf.access.AccessAPIGrpc.AccessAPIImplBase
import java.util.concurrent.TimeUnit

class CachedSporksFlowGrpcApiTest {
    private val properties = FlowBlockchainScannerProperties(chainId = FlowChainId.MAINNET)
    private val blockchainMonitor = mockk<BlockchainMonitor>()
    private val sporkService = SporkService(properties, FlowApiFactoryImpl(blockchainMonitor, properties))
    private val serverInterceptor = mockk<ServerInterceptor>()

    private lateinit var cachedSporksFlowGrpcApi: CachedSporksFlowGrpcApi
    private lateinit var server: Server

    @BeforeEach
    fun before() {
        every {
            serverInterceptor.interceptCall(
                any(),
                any(),
                any<ServerCallHandler<Access.GetLatestBlockRequest, BlockResponse>>()
            )
        } answers {
            (this.thirdArg() as ServerCallHandler<Access.GetLatestBlockRequest, BlockResponse>).startCall(
                this.firstArg(),
                this.secondArg()
            )
        }
        val name = InProcessServerBuilder.generateName()
        val service = object : AccessAPIImplBase() {
            override fun getLatestBlock(
                request: Access.GetLatestBlockRequest,
                responseObserver: StreamObserver<BlockResponse>
            ) {
                responseObserver.onNext(BlockResponse.getDefaultInstance())
                responseObserver.onCompleted()
            }
        }
        server = InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(ServerInterceptors.intercept(service, serverInterceptor))
            .build()
            .start()
        val channel = InProcessChannelBuilder.forName(name)
            .directExecutor()
            .build()
        cachedSporksFlowGrpcApi = CachedSporksFlowGrpcApi(sporkService, properties, FlowApiFactoryStub(channel))
    }

    @AfterEach
    fun tearDown() {
        server.shutdownNow()
        server.awaitTermination(5, TimeUnit.SECONDS)
    }

    @Test
    fun `latest block call with session id`() = runBlocking<Unit> {
        val metadata = slot<Metadata>()
        val block = cachedSporksFlowGrpcApi.latestBlock()
        assertThat(block).isEqualTo(FlowBlock.of(BlockResponse.getDefaultInstance().block))
        verify {
            serverInterceptor.interceptCall(
                any(),
                capture(metadata),
                any<ServerCallHandler<Access.GetLatestBlockRequest, BlockResponse>>()
            )
        }
        assertThat(metadata.captured.get(SESSION_HASH_HEADER)).isNotBlank()
    }
}
