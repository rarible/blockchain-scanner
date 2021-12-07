package com.rarible.blockchain.scanner.flow.client

import com.rarible.blockchain.scanner.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.flow.model.FlowDescriptor
import org.springframework.stereotype.Component

@Component
class FlowRetryableClient(
    client: FlowBlockchainClient,
    properties: BlockchainScannerProperties
) : RetryableBlockchainClient<FlowBlockchainBlock, FlowBlockchainLog, FlowDescriptor>(
    client,
    properties.retryPolicy.client
)
