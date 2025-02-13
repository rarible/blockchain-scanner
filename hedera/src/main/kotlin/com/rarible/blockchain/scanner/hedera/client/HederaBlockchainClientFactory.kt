package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.framework.client.BlockchainClientFactory
import com.rarible.blockchain.scanner.hedera.model.HederaDescriptor
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.springframework.stereotype.Component

@Component
@ExperimentalCoroutinesApi
class HederaBlockchainClientFactory(
    private val defaultClient: HederaBlockchainClient
) : BlockchainClientFactory<HederaBlockchainBlock, HederaBlockchainLog, HederaDescriptor> {

    override fun createMainClient(): HederaBlockchainClient {
        return defaultClient
    }

    override fun createReconciliationClient(): HederaBlockchainClient {
        return defaultClient
    }
}
