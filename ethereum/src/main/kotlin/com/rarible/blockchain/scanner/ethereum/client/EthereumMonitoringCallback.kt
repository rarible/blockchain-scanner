package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.monitoring.BlockchainMonitor
import com.rarible.ethereum.client.monitoring.MonitoringCallback
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class EthereumMonitoringCallback(
    private val blockchainMonitor: BlockchainMonitor,
    private val blockchain: String
) : MonitoringCallback {

    @Autowired
    constructor(
        blockchainMonitor: BlockchainMonitor,
        properties: EthereumScannerProperties,
    ) : this(blockchainMonitor, properties.blockchain)

    override fun <T> onBlockchainCall(method: String, monoCall: () -> Mono<T>): Mono<T> =
        blockchainMonitor.onBlockchainCall(blockchain, method, monoCall)
}
