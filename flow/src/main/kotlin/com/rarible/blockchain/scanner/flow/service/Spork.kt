package com.rarible.blockchain.scanner.flow.service

import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.impl.AsyncFlowAccessApiImpl
import com.rarible.blockchain.scanner.flow.monitoring.FlowMonitor
import io.grpc.HttpConnectProxiedSocketAddress
import io.grpc.ManagedChannelBuilder
import io.grpc.ProxyDetector
import org.onflow.protobuf.access.AccessAPIGrpc
import java.net.InetSocketAddress
import java.net.URI

class Spork private constructor(
    private val from: Long,
    private val to: Long,
    private val nodeUrl: String,
    private val port: Int,
    private val proxy: URI? = null,
    private val flowMonitor: FlowMonitor,
) {
    constructor(
        from: Long,
        to: Long = Long.MAX_VALUE,
        nodeUrl: String,
        port: Int = 9000,
        flowMonitor: FlowMonitor,
    ): this(from = from, to = to, nodeUrl = nodeUrl, port = port, proxy = null, flowMonitor = flowMonitor)

    @Suppress("PrivatePropertyName")
    private val DEFAULT_MESSAGE_SIZE: Int = 33554432 //32 Mb

    val api by lazy {
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
        val channel = ManagedChannelBuilder.forAddress(nodeUrl, port)
            .maxInboundMessageSize(DEFAULT_MESSAGE_SIZE)
            .run { if (detector != null) proxyDetector(detector) else this }
            .usePlaintext()
            .userAgent(Flow.DEFAULT_USER_AGENT)
            .build()
        AsyncFlowAccessApiImpl(AccessAPIGrpc.newFutureStub(channel))
    }

    fun containsBlock(blockHeight: Long): Boolean = blockHeight in from..to

    fun containsBlock(id: FlowId): Boolean =
        try {
            flowMonitor.onBlockchainCall("getBlockHeaderById")
            api.getBlockHeaderById(id).join() != null
        } catch (_: Exception) {
            false
        }

    fun containsTx(id: FlowId): Boolean =
        try {
            flowMonitor.onBlockchainCall("getTransactionById")
            api.getTransactionById(id).join() != null
        } catch (_: Exception) {
            false
        }

    fun trim(range: LongRange): LongRange {
        val first = if (from < range.first) range.first else from
        val last = if (to < range.last) to else range.last
        return first..last
    }

    internal fun withProxy(proxy: URI?): Spork {
        return if (proxy != null)
            Spork(
                from = this.from,
                to = this.to,
                nodeUrl = this.nodeUrl,
                port = this.port,
                proxy = proxy,
                flowMonitor = flowMonitor,
            )
        else this
    }
}