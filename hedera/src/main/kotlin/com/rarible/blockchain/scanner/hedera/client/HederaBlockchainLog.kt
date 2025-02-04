package com.rarible.blockchain.scanner.hedera.client

import com.rarible.blockchain.scanner.framework.client.BlockchainLog
import com.rarible.blockchain.scanner.hedera.client.rest.dto.HederaTransaction
import com.rarible.blockchain.scanner.hedera.client.rest.dto.NftTransfer
import com.rarible.blockchain.scanner.hedera.client.rest.dto.Transfer

class HederaBlockchainLog(
    private val transaction: HederaTransaction,
) : BlockchainLog {

    val transactionHash: String
        get() = transaction.transactionHash

    val entityId: String?
        get() = transaction.entityId

    val name: String
        get() = transaction.name

    val consensusTimestamp: String
        get() = transaction.consensusTimestamp

    val transactionId: String
        get() = transaction.transactionId

    val transfers: List<Transfer>
        get() = transaction.transfers

    val nftTransfers: List<NftTransfer>?
        get() = transaction.nftTransfers
}
