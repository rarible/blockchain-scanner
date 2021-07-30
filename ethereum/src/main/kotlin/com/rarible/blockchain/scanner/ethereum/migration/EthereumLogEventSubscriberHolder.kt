package com.rarible.blockchain.scanner.ethereum.migration

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.subscriber.LogEventSubscriber
import org.springframework.stereotype.Component

@Component
class EthereumLogEventSubscriberHolder(

    val subscribers: List<LogEventSubscriber<EthereumBlockchainLog, EthereumBlockchainBlock>>

)