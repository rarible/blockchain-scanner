package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClientFactory
import com.rarible.blockchain.scanner.solana.configuration.SolanaBlockchainScannerProperties
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import com.rarible.blockchain.scanner.solana.subscriber.SolanaLogEventSubscriber
import org.springframework.stereotype.Component

@Component
class SolanaClientFactory(
    private val properties: SolanaBlockchainScannerProperties,
    private val subscribers: List<SolanaLogEventSubscriber>
) : BlockchainClientFactory<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaDescriptor> {
    override fun createMainClient(): SolanaClient {
        return SolanaClient(
            api = SolanaHttpRpcApi(urls = properties.rpcApiUrls, timeoutMillis = properties.rpcApiTimeout),
            properties = properties,
            filters = subscribers.map { it.getDescriptor().filter }.toSet()
        )
    }

    override fun createReconciliationClient(): SolanaClient {
        return SolanaClient(
            api = SolanaHttpRpcApi(
                urls = properties.reconciliationRpcApiUrls,
                timeoutMillis = properties.rpcApiTimeout
            ),
            properties = properties,
            filters = subscribers.map { it.getDescriptor().filter }.toSet()
        )
    }
}
