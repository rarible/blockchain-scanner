package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.ethereum.configuration.EthereumScannerProperties
import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClientFactory
import org.springframework.stereotype.Component
import scalether.core.MonoEthereum

@Component
class EthereumClientFactory(
    private val mainEthereum: MonoEthereum,
    private val reconciliationEthereum: MonoEthereum,
    private val properties: EthereumScannerProperties,
) : BlockchainClientFactory<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumDescriptor> {
    override fun createMainClient(): EthereumClient = EthereumClient(mainEthereum, properties)
    override fun createReconciliationClient(): EthereumClient = EthereumClient(reconciliationEthereum, properties)
}
