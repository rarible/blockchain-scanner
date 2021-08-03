package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import kotlinx.coroutines.flow.Flow
import org.bson.types.ObjectId

interface LogService<L : Log, D : Descriptor> {

    suspend fun delete(descriptor: D, log: L): L

    suspend fun saveOrUpdate(descriptor: D, event: L): L

    suspend fun save(descriptor: D, log: L): L

    fun findPendingLogs(descriptor: D): Flow<L>

    suspend fun findLogEvent(descriptor: D, id: ObjectId): L

    fun findAndRevert(descriptor: D, blockHash: String): Flow<L>

    fun findAndDelete(descriptor: D, blockHash: String, status: Log.Status? = null): Flow<L>

    suspend fun updateStatus(descriptor: D, log: L, status: Log.Status): L


}