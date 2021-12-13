package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.BlockchainScannerProperties
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import org.springframework.stereotype.Component

@Component
class SolanaRetryableClient(
    client: SolanaClient,
    properties: BlockchainScannerProperties
) : RetryableBlockchainClient<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaDescriptor>(
    client,
    properties.retryPolicy.client
)
