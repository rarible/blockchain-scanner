package com.rarible.blockchain.scanner.framework.service

import com.rarible.blockchain.scanner.framework.model.Descriptor
import com.rarible.blockchain.scanner.framework.model.Log
import com.rarible.blockchain.scanner.framework.model.LogRecord
import kotlinx.coroutines.flow.Flow

interface LogService<L : Log, R : LogRecord<L>, D : Descriptor> {

    suspend fun delete(descriptor: D, record: R): R

    suspend fun saveOrUpdate(descriptor: D, record: R): R

    suspend fun save(descriptor: D, record: R): R

    fun findPendingLogs(descriptor: D): Flow<R>

    fun findAndRevert(descriptor: D, blockHash: String): Flow<R>

    fun findAndDelete(descriptor: D, blockHash: String, status: Log.Status? = null): Flow<R>

    suspend fun updateStatus(descriptor: D, record: R, status: Log.Status): R


}