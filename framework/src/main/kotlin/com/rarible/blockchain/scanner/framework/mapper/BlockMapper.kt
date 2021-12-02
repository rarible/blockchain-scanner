package com.rarible.blockchain.scanner.framework.mapper

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.Block

/**
 * Mapper (i.e. converter) of original Blockchain Block to persistent Block, which will be stored in storage of specific
 * BlockchainScanner implementation.
 */
interface BlockMapper<BB : BlockchainBlock, B : Block> {

    /**
     * Convert original Blockchain Block to persistent Block data
     */
    fun map(originalBlock: BB): B

}