package com.rarible.blockchain.scanner.ethereum.migration

import com.github.cloudyrock.mongock.ChangeLog
import com.github.cloudyrock.mongock.ChangeSet
import com.github.cloudyrock.mongock.driver.mongodb.springdata.v3.decorator.impl.MongockTemplate
import com.rarible.blockchain.scanner.ethereum.model.EthereumBlock
import io.changock.migration.api.annotations.NonLockGuarded
import org.bson.Document
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.index.Index
import org.springframework.data.mongodb.core.index.PartialIndexFilter

@ChangeLog(order = "00001")
class ChangeLog00001 {

    @ChangeSet(id = "addBlockIndex", order = "00001", author = "eugene")
    fun addBlockIndex(template: MongockTemplate) {
        template.indexOps(EthereumBlock::class.java).ensureIndex(
            Index().on("hash", Sort.Direction.ASC)
        )
    }

    @ChangeSet(id = "ensureInitialIndexes", order = "00002", author = "eugene")
    fun ensureInitialIndexes(
        template: MongockTemplate,
        @NonLockGuarded holderEthereum: EthereumLogEventSubscriberHolder
    ) {
        val collections = holderEthereum.subscribers.map { it.getDescriptor().collection }.toSet()
        collections.forEach { createInitialIndices(template, it) }
    }

    fun createInitialIndices(template: MongockTemplate, collection: String) {
        val indexOps = template.indexOps(collection)
        indexOps.ensureIndex(
            Index()
                .on("transactionHash", Sort.Direction.ASC)
                .on("topic", Sort.Direction.ASC)
                .on("address", Sort.Direction.ASC)
                .on("index", Sort.Direction.ASC)
                .on("minorLogIndex", Sort.Direction.ASC)
                .on("visible", Sort.Direction.ASC)
                .named(VISIBLE_INDEX_NAME)
                .background()
                .unique()
                .partial(PartialIndexFilter.of(Document("visible", true)))
        )

        // This index is not used for queries but only to ensure the consistency of the database.
        indexOps.ensureIndex(
            Index()
                .on("transactionHash", Sort.Direction.ASC)
                .on("blockHash", Sort.Direction.ASC)
                .on("logIndex", Sort.Direction.ASC)
                .on("minorLogIndex", Sort.Direction.ASC)
                .named("transactionHash_1_blockHash_1_logIndex_1_minorLogIndex_1")
                .background()
                .unique()
        )

        indexOps.ensureIndex(
            Index()
                .on("status", Sort.Direction.ASC)
                .named("status")
                .background()
        )

        indexOps.ensureIndex(
            Index()
                .on("blockHash", Sort.Direction.ASC)
                .named("blockHash")
                .background()
        )
    }

    companion object {
        const val VISIBLE_INDEX_NAME =
            "transactionHash_1_topic_1_address_1_index_1_minorLogIndex_1_visible_1"
    }
}
