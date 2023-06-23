package com.rarible.blockchain.scanner.test.model

import com.rarible.blockchain.scanner.framework.model.TransactionRecord

data class TestTransactionRecord(
    val hash: String,
    val input: String,
) : TransactionRecord {

    override fun getKey(): String = hash
}