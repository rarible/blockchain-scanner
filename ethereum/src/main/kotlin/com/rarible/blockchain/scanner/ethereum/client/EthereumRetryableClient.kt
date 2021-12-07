package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import org.springframework.stereotype.Component

@Component
class EthereumRetryableClient(
    private val client: EthereumClient,
    properties: BlockchainScannerProperties
) : EthereumBlockchainClient,
    RetryableBlockchainClient<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumDescriptor>(
        client,
        properties.retryPolicy.client
    ) {

    override suspend fun getBlock(hash: String): EthereumBlockchainBlock {
        return wrapWithRetry("getBlock", hash) {
            client.getBlock(hash)
        }
    }
}