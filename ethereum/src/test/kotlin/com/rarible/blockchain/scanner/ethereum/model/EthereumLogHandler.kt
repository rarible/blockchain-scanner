package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainBlock
import com.rarible.blockchain.scanner.ethereum.client.EthereumBlockchainLog
import com.rarible.blockchain.scanner.handler.LogHandler

internal typealias EthereumLogHandler = LogHandler<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumLogRecord, EthereumDescriptor>
