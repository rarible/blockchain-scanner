package com.rarible.blockchain.scanner

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking

fun simple(): Flow<Int> = flow {
    println("Started simple flow")
    for (i in 1..10) {
        println("emitting $i")
        emit(i)
    }
}.buffer(3)

fun main() = runBlocking<Unit> {
    simple()
        .map {
            println("mapping $it")
            it
        }
        .collect { value -> println("Collected $value") }
}