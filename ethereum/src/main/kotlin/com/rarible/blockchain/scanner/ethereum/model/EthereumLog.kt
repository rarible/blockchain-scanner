package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.Log
import io.daonomic.rpc.domain.Word
import scalether.domain.Address

data class EthereumLog(

    override val transactionHash: String,
    override val status: Log.Status,
    val topic: Word,

    val from: Address? = null,
    val nonce: Long? = null,
    val minorLogIndex: Int,
    val index: Int,
    val address: Address,
    val blockHash: Word? = null,
    val blockNumber: Long? = null,
    val logIndex: Int? = null,
    val visible: Boolean = true

) : Log

