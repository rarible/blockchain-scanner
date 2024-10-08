package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.Flow
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.HttpConnectProxiedSocketAddress
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.ProxyDetector
import io.grpc.stub.MetadataUtils
import org.onflow.protobuf.access.AccessAPIGrpc
import java.net.InetSocketAddress
import java.net.URI
import java.util.concurrent.TimeUnit
import kotlin.reflect.KProperty

class FlowApiFactoryImpl(
    private val blockchainMonitor: BlockchainMonitor,
    private val flowBlockchainScannerProperties: FlowBlockchainScannerProperties,
    private val urlProvider: KProperty<String> = Spork::nodeUrl
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
        val timeout = flowBlockchainScannerProperties.timeout.toMillis()
        val metadata = Metadata()
        if (spork.headers.isNotEmpty()) {
            spork.headers.forEach { (key, value) ->
                if (value.isNotBlank()) {
                    metadata.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value)
                }
            }
        }
        val headerInterceptor = MetadataUtils.newAttachHeadersInterceptor(metadata)
        val channel = ManagedChannelBuilder.forAddress(urlProvider.getter.call(spork), spork.port)
            .maxInboundMessageSize(DEFAULT_MESSAGE_SIZE)
            .run { if (detector != null) proxyDetector(detector) else this }
            .usePlaintext()
            .userAgent(Flow.DEFAULT_USER_AGENT)
            .intercept(
                headerInterceptor,
                object : ClientInterceptor {
                    override fun <ReqT : Any?, RespT : Any?> interceptCall(
                        descriptor: MethodDescriptor<ReqT, RespT>?,
                        options: CallOptions,
                        channel: Channel
                    ): ClientCall<ReqT, RespT> {
                        return channel.newCall(descriptor, options.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS))
                    }
                })
            .build()
        val api = AsyncFlowAccessApiImpl(AccessAPIGrpc.newFutureStub(channel))
        return MonitoredFlowApi(delegate = api, closeable = api, blockchainMonitor = blockchainMonitor)
    }

    companion object {
        const val DEFAULT_MESSAGE_SIZE: Int = 33554432
    }
}
