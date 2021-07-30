package com.rarible.blockchain.scanner.ethereum.migration

import com.github.cloudyrock.mongock.ChangeLog
import com.github.cloudyrock.mongock.ChangeSet
import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.index.Index

@ChangeLog(order = "00002")
class ChangeLog00002 {

    @ChangeSet(id = "ChangeLog00002.addBlockIndex", order = "0000", author = "eugene")
    fun addBlockIndex(template: MongockTemplate) {
        template.indexOps(EthereumBlock::class.java).ensureIndex(
            Index()
                .on("status", Sort.Direction.ASC)
        )
        template.indexOps(EthereumBlock::class.java).ensureIndex(
            Index()
                .on("hash", Sort.Direction.ASC)
        )
    }
}