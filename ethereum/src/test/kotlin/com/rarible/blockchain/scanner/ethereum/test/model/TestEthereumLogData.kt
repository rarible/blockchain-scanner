package com.rarible.blockchain.scanner.ethereum.test.model

import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EventData
import scalether.domain.Address
import java.math.BigInteger

data class TestEthereumLogData(
    val customData: String,
    val from: Address,
    val to: Address,
    val value: BigInteger
) : EventData {

    override fun getKey(log: EthereumLog): String {
        return from.hex()
    }
}