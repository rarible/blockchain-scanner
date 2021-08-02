package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.LogEventDescriptor
import scalether.domain.Address

class EthereumLogEventDescriptor(
    override val topic: String,
    val collection: String,
    val contracts: List<Address>
) : LogEventDescriptor
