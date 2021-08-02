package com.rarible.blockchain.scanner.ethereum.subscriber

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogEventDescriptor
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber

interface EthereumLogEventSubscriber :
    LogEventSubscriber<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogEventDescriptor> {
}