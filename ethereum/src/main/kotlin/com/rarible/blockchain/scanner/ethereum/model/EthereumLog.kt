package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.Log
import io.daonomic.rpc.domain.Word

data class EthereumLog(

    override val transactionHash: String,
    override val status: Log.Status,
    val topic: String,

    val from: String? = null,
    val nonce: Long? = null,
    val minorLogIndex: Int,
    val index: Int,
    val address: String,
    val blockHash: Word? = null,
    val blockNumber: Long? = null,
    val logIndex: Int? = null,
    val visible: Boolean = true

) : Log

