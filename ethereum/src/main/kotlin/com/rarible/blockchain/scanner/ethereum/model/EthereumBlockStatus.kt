package com.rarible.blockchain.scanner.ethereum.model

enum class EthereumBlockStatus {
    CONFIRMED,
    REVERTED,
    @Deprecated("Shouldn't be use, need remove after release")
    PENDING,
    @Deprecated("Shouldn't be use, need remove after release")
    DROPPED,
    @Deprecated("Shouldn't be use, need remove after release")
    INACTIVE
}
