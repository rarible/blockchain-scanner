package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.RetryableBlockchainClient
import com.rarible.blockchain.scanner.solana.configuration.SolanaBlockchainScannerProperties
import com.rarible.blockchain.scanner.solana.model.SolanaDescriptor
import org.springframework.stereotype.Component

@Component
class SolanaRetryableClient(
    properties: SolanaBlockchainScannerProperties
) : RetryableBlockchainClient<SolanaBlockchainBlock, SolanaBlockchainLog, SolanaDescriptor>(
    SolanaClient(properties.rpcApiUrl),
    properties.retryPolicy.client
)
