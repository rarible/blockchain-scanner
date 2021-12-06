package com.rarible.blockchain.scanner.ethereum.test.model

import com.rarible.blockchain.scanner.ethereum.model.EthereumLog
import com.rarible.blockchain.scanner.ethereum.model.EthereumLogRecord
import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version
import scalether.domain.Address
import java.math.BigInteger

data class TestEthereumLogRecord(
    @Id
    override val id: ObjectId,
    @Version
    override val version: Long? = null,
    override val log: EthereumLog? = null,
    val customData: String,
    val from: Address,
    val to: Address,
    val value: BigInteger
) : EthereumLogRecord<TestEthereumLogRecord>() {

    override fun withLog(log: EthereumLog): TestEthereumLogRecord {
        return copy(log = log)
    }

    override fun withIdAndVersion(id: ObjectId, version: Long?): TestEthereumLogRecord {
        return copy(id = id, version = version)
    }

    override fun getKey(): String {
        return from.hex()
    }
}