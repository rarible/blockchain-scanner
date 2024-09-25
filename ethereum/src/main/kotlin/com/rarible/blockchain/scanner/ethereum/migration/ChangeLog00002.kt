package com.rarible.blockchain.scanner.ethereum.migration

import com.github.cloudyrock.mongock.ChangeLog
import com.github.cloudyrock.mongock.ChangeSet
import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import io.changock.migration.api.annotations.NonLockGuarded

@ChangeLog(order = "00002")
class ChangeLog00002 {

    @ChangeSet(id = "00002_dropStatusIndex", order = "00001", author = "igorbaltiyskiy-rari")
    fun dropStatusIndex(
        mongockTemplate: MongockTemplate,
        @NonLockGuarded subscriberHolder: EthereumLogEventSubscriberHolder
    ) {
        val repositories = subscriberHolder.subscribers.map { it.getDescriptor().storage }.toSet()
        repositories.forEach {
            it.dropStatusIndex(mongockTemplate)
        }
    }
}
