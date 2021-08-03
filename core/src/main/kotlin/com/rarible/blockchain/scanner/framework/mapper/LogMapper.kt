package com.rarible.blockchain.scanner.framework.mapper

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log

interface LogMapper<BB : BlockchainBlock, BL : BlockchainLog, L : Log> {

    fun map(
        block: BB,
        log: BL,
        index: Int,
        minorIndex: Int,
        descriptor: Descriptor
    ): L
}