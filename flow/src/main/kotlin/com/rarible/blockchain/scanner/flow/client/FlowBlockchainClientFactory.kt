package com.rarible.blockchain.scanner.flow.client

import com.rarible.blockchain.scanner.flow.CachedSporksFlowGrpcApi
import com.rarible.blockchain.scanner.flow.FlowGrpcApi
import com.rarible.blockchain.scanner.flow.FlowNetNewBlockPoller
import com.rarible.blockchain.scanner.flow.configuration.FlowBlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.http.FlowHttpApi
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import com.rarible.blockchain.scanner.flow.service.CachingFlowApiFactory
import com.rarible.blockchain.scanner.flow.service.FlowApiFactoryImpl
import com.rarible.blockchain.scanner.flow.service.Spork
import com.rarible.blockchain.scanner.flow.service.SporkService
import com.rarible.blockchain.scanner.framework.client.BlockchainClientFactory
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import org.springframework.stereotype.Component
import java.io.Closeable

@Component
class FlowBlockchainClientFactory(
    blockchainMonitor: BlockchainMonitor,
    private val httpApi: FlowHttpApi,
    private val properties: FlowBlockchainScannerProperties
) : BlockchainClientFactory<FlowBlockchainBlock, FlowBlockchainLog, FlowDescriptor>, Closeable {
    final val apiFactory: CachingFlowApiFactory = CachingFlowApiFactory(
        FlowApiFactoryImpl(
            blockchainMonitor = blockchainMonitor,
            flowBlockchainScannerProperties = properties,
        )
    )

    private val reconciliationApiFactory = CachingFlowApiFactory(
        FlowApiFactoryImpl(
            blockchainMonitor = blockchainMonitor,
            flowBlockchainScannerProperties = properties,
            urlProvider = Spork::reconciliationNodeUrl,
        )
    )

    final val sporkService: SporkService = SporkService(
        flowApiFactory = apiFactory,
        properties = properties,
    )

    final val flowGrpcApi: FlowGrpcApi = CachedSporksFlowGrpcApi(
        flowApiFactory = apiFactory,
        sporkService = sporkService,
        properties = properties,
    )

    override fun createMainClient(): FlowBlockchainClient {
        return FlowBlockchainClient(
            poller = FlowNetNewBlockPoller(
                properties = properties,
                api = flowGrpcApi,
            ),
            api = flowGrpcApi,
            httpApi = httpApi,
            properties = properties,
        )
    }

    override fun createReconciliationClient(): FlowBlockchainClient {
        return FlowBlockchainClient(
            poller = FlowNetNewBlockPoller(
                properties = properties,
                api = flowGrpcApi,
            ),
            api = CachedSporksFlowGrpcApi(
                flowApiFactory = apiFactory,
                sporkService = SporkService(
                    flowApiFactory = reconciliationApiFactory,
                    properties = properties,
                ),
                properties = properties,
            ),
            httpApi = httpApi,
            properties = properties,
        )
    }

    override fun close() {
        try {
            apiFactory.close()
        } catch (e: Exception) {
        }
        try {
            reconciliationApiFactory.close()
        } catch (e: Exception) {
        }
    }
}
