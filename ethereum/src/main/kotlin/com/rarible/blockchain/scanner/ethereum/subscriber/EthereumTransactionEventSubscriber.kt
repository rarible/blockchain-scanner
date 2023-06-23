package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.framework.model.TransactionRecord
import com.rarible.blockchain.scanner.framework.subscriber.TransactionEventSubscriber

interface EthereumTransactionEventSubscriber : TransactionEventSubscriber<EthereumBlockchainBlock, TransactionRecord>
