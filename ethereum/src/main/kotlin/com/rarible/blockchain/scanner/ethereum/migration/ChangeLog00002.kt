package com.rarible.blockchain.scanner.ethereum.migration

import com.github.cloudyrock.mongock.ChangeLog
import com.github.cloudyrock.mongock.ChangeSet
import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import io.changock.migration.api.annotations.NonLockGuarded

@ChangeLog(order = "00002")
class ChangeLog00002 {
    companion object {
      const val STATUS_INDEX_NAME = "status"
    }

    @ChangeSet(id = "00002_dropStatusIndex", order = "00001", author = "igorbaltiyskiy-rari")
    fun dropStatusIndex(
        mongockTemplate: MongockTemplate,
        @NonLockGuarded subscriberHolder: EthereumLogEventSubscriberHolder,
    ) {
        val uniqueCollections = subscriberHolder.subscribers.map { it.getDescriptor().collection }.toSet()
        uniqueCollections.forEach { collection ->
            // We don't have guarantees from Mongock that we didn't fail on this step before, so the index might have been dropped already
            val indexOps = mongockTemplate.indexOps(collection)
            if (indexOps.indexInfo.any { it.name == STATUS_INDEX_NAME }) {
              indexOps.dropIndex(STATUS_INDEX_NAME)
            }
        }
    }
}
