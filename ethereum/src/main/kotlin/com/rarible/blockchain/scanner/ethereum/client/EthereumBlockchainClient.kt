package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.ethereum.model.EthereumDescriptor
import com.rarible.blockchain.scanner.framework.client.BlockchainClient

interface EthereumBlockchainClient : BlockchainClient<EthereumBlockchainBlock, EthereumBlockchainLog, EthereumDescriptor>
