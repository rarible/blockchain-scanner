package com.rarible.blockchain.scanner.configuration

data class RetryPolicyProperties(
    val scan: ScanRetryPolicyProperties = ScanRetryPolicyProperties(),
    val client: ClientRetryPolicyProperties = ClientRetryPolicyProperties()
)