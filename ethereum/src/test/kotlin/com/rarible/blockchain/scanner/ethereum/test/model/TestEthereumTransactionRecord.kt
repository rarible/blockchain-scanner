package com.rarible.blockchain.scanner.ethereum.test.model

import com.rarible.blockchain.scanner.framework.model.TransactionRecord

data class TestEthereumTransactionRecord(
    val hash: String,
    val input: String,
) : TransactionRecord {
    override fun getKey(): String = hash
}
