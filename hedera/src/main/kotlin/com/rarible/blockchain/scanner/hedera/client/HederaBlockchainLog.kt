package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransaction
import com.rarible.blockchain.scanner.hedera.model.HederaLog

class HederaBlockchainLog(
    val log: HederaLog,
    val transaction: HederaTransaction,
) : BlockchainLog
