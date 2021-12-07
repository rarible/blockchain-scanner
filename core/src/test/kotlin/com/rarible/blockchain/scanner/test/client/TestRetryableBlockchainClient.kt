package com.rarible.blockchain.scanner.test.client

import com.rarible.blockchain.scanner.RetryableBlockchainClient
import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import com.rarible.blockchain.scanner.test.model.TestDescriptor

class TestRetryableBlockchainClient(client: TestBlockchainClient) :
    RetryableBlockchainClient<TestBlockchainBlock, TestBlockchainLog, TestDescriptor>(
        client,
        ClientRetryPolicyProperties()
    )