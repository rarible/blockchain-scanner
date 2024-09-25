package com.rarible.blockchain.scanner.flow.migration

import com.github.cloudyrock.mongock.ChangeLog
import com.github.cloudyrock.mongock.ChangeSet
import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import io.changock.migration.api.annotations.NonLockGuarded

@ChangeLog(order = "99999")
class ChangeLog99999CreateIndexes {
    @ChangeSet(
        id = "blockchain-scanner.ChangeLog99999CreateIndexes.createIndexesForAllSubscriberStorages",
        author = "ibaltiyskiy",
        order = "99999",
        runAlways = true,
    )
    fun createIndexesForAllSubscriberStorages(
        mongockTemplate: MongockTemplate,
        @NonLockGuarded subscriberHolder: FlowLogEventSubscriberHolder
    ) {
        val repositories = subscriberHolder.subscribers.map { it.getDescriptor().storage }.toSet()
        repositories.forEach {
            it.createIndexes(mongockTemplate)
        }
    }
}
