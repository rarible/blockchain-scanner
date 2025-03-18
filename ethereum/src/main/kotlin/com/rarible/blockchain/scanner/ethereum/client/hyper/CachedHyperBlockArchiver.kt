package com.rarible.blockchain.scanner.ethereum.client.hyper

import com.github.benmanes.caffeine.cache.AsyncLoadingCache
import com.github.benmanes.caffeine.cache.Caffeine
import com.rarible.blockchain.scanner.ethereum.configuration.HyperProperties
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactor.mono
import java.math.BigInteger

class CachedHyperBlockArchiver(
    private val hyperBlockArchiver: HyperBlockArchiver,
    properties: HyperProperties
) {
    private val cache: AsyncLoadingCache<BigInteger, HyperBlock> = Caffeine.newBuilder()
        .maximumSize(properties.cache.size)
        .expireAfterWrite(properties.cache.expireAfterWrite)
        .buildAsync { key, _ -> fetchBlock(key).toFuture() }

    suspend fun downloadBlock(blockNumber: BigInteger): HyperBlock {
        return cache.get(blockNumber).await()
    }

    suspend fun downloadBlocks(blocks: List<BigInteger>): List<HyperBlock> {
        return cache.getAll(blocks).await().values.toList()
    }

    private fun fetchBlock(blockNumber: BigInteger) = mono {
        hyperBlockArchiver.downloadBlock(blockNumber)
    }
}
