package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.ethereum.repository.EthereumLogRepository
import com.rarible.blockchain.scanner.framework.model.Descriptor
import io.daonomic.rpc.domain.Word
import scalether.domain.Address

data class EthereumDescriptor(
    val ethTopic: Word,
    override val groupId: String,
    val contracts: List<Address>,
    override val id: String = ethTopic.toString(),
    override val alias: String? = null,
    override val storage: EthereumLogRepository,
    val saveLogs: Boolean = true,
) : Descriptor<EthereumLogRepository> {
    override fun shouldSaveLogs(): Boolean {
        return saveLogs
    }
}
