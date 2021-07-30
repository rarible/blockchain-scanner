package com.rarible.blockchain.scanner.framework.mapper

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.Block

interface BlockMapper<BB : BlockchainBlock, B : Block> {

    fun map(originalBlock: BB): B

}