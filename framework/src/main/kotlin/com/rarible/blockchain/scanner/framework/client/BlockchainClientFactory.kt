package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.framework.model.Descriptor

interface BlockchainClientFactory<BB : BlockchainBlock, BL : BlockchainLog, D : Descriptor<*>> {
    fun createMainClient(): BlockchainClient<BB, BL, D>
    fun createReconciliationClient(): BlockchainClient<BB, BL, D>
}
