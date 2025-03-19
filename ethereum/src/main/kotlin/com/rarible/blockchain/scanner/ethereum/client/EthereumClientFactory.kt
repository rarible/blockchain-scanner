package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClient
import com.rarible.blockchain.scanner.framework.client.BlockchainClientFactory
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.springframework.stereotype.Component
import scalether.core.MonoEthereum

@Component
@Suppress("SpringJavaInjectionPointsAutowiringInspection")
class EthereumClientFactory(
    private val mainEthereum: MonoEthereum,
    private val reconciliationEthereum: MonoEthereum,
    private val properties: EthereumScannerProperties,
) : BlockchainClientFactory<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumDescriptor> {

    @ExperimentalCoroutinesApi
    override fun createMainClient(): EthereumClient = EthereumClient(mainEthereum, properties)

    @ExperimentalCoroutinesApi
    override fun createReconciliationClient(): EthereumClient = EthereumClient(reconciliationEthereum, properties)

    @ExperimentalCoroutinesApi
    override fun createReindexClient(): BlockchainClient<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumDescriptor> {
        return if (properties.hyperArchive.enabled) createHyperArchiveEthereumClient() else createMainClient()
    }

    private fun createHyperArchiveEthereumClient(): EthereumBlockchainClient {
        TODO()
    }
}
