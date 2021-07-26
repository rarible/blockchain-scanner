package com.rarible.blockchain.scanner.framework.mapper

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.Block

interface BlockMapper<OB : BlockchainBlock, B : Block> {

    fun map(originalBlock: OB): B

}