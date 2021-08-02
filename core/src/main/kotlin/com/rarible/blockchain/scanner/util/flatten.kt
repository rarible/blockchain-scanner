package com.rarible.blockchain.scanner.util

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow

fun <T> flatten(fn: suspend () -> Flow<T>): Flow<T> = flow {
    emitAll(fn())
}