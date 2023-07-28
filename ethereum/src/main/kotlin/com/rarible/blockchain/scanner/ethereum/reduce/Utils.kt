package com.rarible.blockchain.scanner.ethereum.reduce

import com.rarible.core.entity.reducer.chain.ReducersChain
import com.rarible.core.entity.reducer.service.Reducer

fun <Event, Entity> combineIntoChain(
    vararg reducers: Reducer<Event, Entity>,
): Reducer<Event, Entity> {
    return ReducersChain(reducers.asList())
}
