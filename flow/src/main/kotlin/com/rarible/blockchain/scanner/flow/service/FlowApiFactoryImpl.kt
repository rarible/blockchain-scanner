package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.Flow
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import io.grpc.HttpConnectProxiedSocketAddress
import io.grpc.ManagedChannelBuilder
import io.grpc.ProxyDetector
import org.onflow.protobuf.access.AccessAPIGrpc
import org.springframework.stereotype.Component
import java.net.InetSocketAddress
import java.net.URI
import java.util.concurrent.TimeUnit

@Component
class FlowApiFactoryImpl(
    private val blockchainMonitor: BlockchainMonitor,
    private val flowBlockchainScannerProperties: FlowBlockchainScannerProperties,
) : FlowApiFactory {

    override fun getApi(spork: Spork): AsyncFlowAccessApi {
        val proxy = flowBlockchainScannerProperties.proxy?.let { URI(it) }
        val detector = proxy?.let {
            val userInfo = proxy.userInfo.split(":")
            val proxyAddress = InetSocketAddress(proxy.host, proxy.port)
            ProxyDetector {
                if (it !is InetSocketAddress) return@ProxyDetector null
                HttpConnectProxiedSocketAddress.newBuilder()
                    .setUsername(userInfo[0])
                    .setPassword(userInfo[1])
                    .setProxyAddress(proxyAddress)
                    .setTargetAddress(it)
                    .build()
            }
        }
        val channel = ManagedChannelBuilder.forAddress(spork.nodeUrl, spork.port)
            .maxInboundMessageSize(DEFAULT_MESSAGE_SIZE)
            .run { if (detector != null) proxyDetector(detector) else this }
            .usePlaintext()
            .userAgent(Flow.DEFAULT_USER_AGENT)
            .build()
        val api = AsyncFlowAccessApiImpl(
            AccessAPIGrpc.newFutureStub(channel)
                .withDeadlineAfter(flowBlockchainScannerProperties.timeout.toMillis(), TimeUnit.MILLISECONDS)
        )
        return MonitoredFlowApi(delegate = api, closeable = api, blockchainMonitor = blockchainMonitor)
    }

    companion object {
        const val DEFAULT_MESSAGE_SIZE: Int = 33554432
    }
}
