package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.Descriptor
import io.daonomic.rpc.domain.Word
import scalether.domain.Address

class EthereumDescriptor(
    val topic: Word,
    override val groupId: String,
    val collection: String,
    val contracts: List<Address>
) : Descriptor {

    override val id: String get() = topic.toString()
}
