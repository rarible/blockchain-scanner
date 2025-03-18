package com.rarible.blockchain.scanner.ethereum.client.hyper

import scalether.domain.response.Block
import scalether.domain.response.Transaction
import java.math.BigInteger

class HyperBlockArchiverAdapter(
    private val hyperBlockArchiver: HyperBlockArchiver,
) {
    suspend fun getBlock(blockNumber: BigInteger): Block<Transaction> {
        return convert(hyperBlockArchiver.downloadBlock(blockNumber))
    }

    private fun convert(hyperBlock: HyperBlock): Block<Transaction> {
        TODO()
    }
}
