package com.rarible.blockchain.scanner.framework.client

import com.rarible.blockchain.scanner.framework.model.Descriptor

/**
 * Blockchain Client - implement it to support new Blockchain
 */
interface BlockchainClient<BB : BlockchainBlock, BL : BlockchainLog, D : Descriptor> :
    BlockchainBlockClient<BB>,
    BlockchainLogClient<BB, BL, D>

