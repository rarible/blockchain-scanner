package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import scalether.domain.response.Log
import scalether.domain.response.Transaction

data class EthereumBlockchainLog(
    val ethLog: Log,
    val ethTransaction: Transaction,
    val index: Int
) : BlockchainLog
