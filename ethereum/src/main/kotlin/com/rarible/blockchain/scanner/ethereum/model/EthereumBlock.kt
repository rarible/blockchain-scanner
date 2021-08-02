package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.Block
import org.springframework.data.annotation.Id

class EthereumBlock(
    @Id
    override val id: Long,

    override val hash: String,
    override val timestamp: Long,
    override val status: Block.Status = Block.Status.PENDING
) : Block