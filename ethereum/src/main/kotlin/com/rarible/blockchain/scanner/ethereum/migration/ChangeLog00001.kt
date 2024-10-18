package com.rarible.blockchain.scanner.ethereum.migration

import com.github.cloudyrock.mongock.ChangeLog
import com.github.cloudyrock.mongock.ChangeSet
import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import com.rarible.blockchain.scanner.block.Block
import io.changock.migration.api.annotations.NonLockGuarded
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.index.Index

@ChangeLog(order = "00001")
class ChangeLog00001 {

    @ChangeSet(id = "addBlockIndex", order = "00001", author = "eugene")
    fun addBlockIndex(template: MongockTemplate) {
        template.indexOps(Block::class.java).ensureIndex(
            Index().on("hash", Sort.Direction.ASC).background()
        )
        template.indexOps(Block::class.java).ensureIndex(
            Index().on("status", Sort.Direction.ASC).background()
        )
    }

    @ChangeSet(id = "ensureInitialIndexes", order = "00002", author = "eugene", runAlways = true)
    fun ensureInitialIndexes(
        template: MongockTemplate,
        @NonLockGuarded subscriberHolder: EthereumLogEventSubscriberHolder
    ) {
        val repositories = subscriberHolder.subscribers.map { it.getDescriptor().storage }.toSet()
        repositories.forEach {
            it.createIndexes(template)
        }
    }
}
