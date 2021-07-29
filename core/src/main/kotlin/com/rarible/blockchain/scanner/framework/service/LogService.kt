package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.model.Log
import kotlinx.coroutines.flow.Flow
import org.bson.types.ObjectId

interface LogService<L : Log> {

    suspend fun delete(collection: String, log: L): L

    suspend fun saveOrUpdate(collection: String, event: L): L

    suspend fun save(collection: String, log: L): L

    fun findPendingLogs(collection: String): Flow<L>

    suspend fun findLogEvent(collection: String, id: ObjectId): L

    fun findAndRevert(collection: String, blockHash: String, topic: String): Flow<L>

    fun findAndDelete(collection: String, blockHash: String, topic: String, status: Log.Status? = null): Flow<L>

    suspend fun updateStatus(collection: String, log: L, status: Log.Status): L


}