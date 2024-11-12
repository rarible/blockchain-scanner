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
        @NonLockGuarded repositoryHolder: EthereumLogRepositoryHolder
    ) {
        val repositories = repositoryHolder.repositories
        repositories.forEach {
            it.dropIndexes(mongockTemplate)
        }
    }
}
