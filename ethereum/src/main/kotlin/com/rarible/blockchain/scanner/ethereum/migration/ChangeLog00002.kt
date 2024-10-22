package com.rarible.blockchain.scanner.ethereum.migration

import com.github.cloudyrock.mongock.ChangeLog
import com.github.cloudyrock.mongock.ChangeSet
import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import io.changock.migration.api.annotations.NonLockGuarded

@ChangeLog(order = "00002")
class ChangeLog00002 {

    @ChangeSet(id = "ChangeLog00002.dropIndexes", order = "00001", author = "protocol", runAlways = true)
    fun dropIndexes(
        mongockTemplate: MongockTemplate,
        @NonLockGuarded subscriberHolder: EthereumLogEventSubscriberHolder
    ) {
        val repositories = subscriberHolder.subscribers.map { it.getDescriptor().storage }.toSet()
        repositories.forEach {
            it.dropIndexes(mongockTemplate)
        }
    }
}
