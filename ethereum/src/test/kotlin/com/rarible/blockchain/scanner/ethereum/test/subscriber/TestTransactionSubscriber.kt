package com.rarible.blockchain.scanner.ethereum.test.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.subscriber.EthereumTransactionEventSubscriber
import com.rarible.blockchain.scanner.ethereum.test.model.TestEthereumTransactionRecord
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import scala.jdk.javaapi.CollectionConverters
import scalether.domain.response.Transaction

class TestTransactionSubscriber : EthereumTransactionEventSubscriber {

    override fun getGroup(): String = "transactions"

    override suspend fun getEventRecords(block: EthereumBlockchainBlock): List<TransactionRecord> {
        val transactions: List<Transaction> = CollectionConverters.asJava(block.ethBlock.transactions())
        return transactions
            .map {
                TestEthereumTransactionRecord(
                    hash = it.hash().toString(),
                    input = it.input().toString(),
                )
            }
    }
}
