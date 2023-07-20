package com.rarible.blockchain.scanner.ethereum.client

import com.rarible.blockchain.scanner.client.AbstractRetryableClient
import com.rarible.blockchain.scanner.configuration.ClientRetryPolicyProperties
import io.daonomic.rpc.domain.Word
import reactor.core.publisher.Mono
import scalether.core.MonoEthereum
import scalether.domain.request.LogFilter
import scalether.domain.response.Block
import scalether.domain.response.Log
import scalether.domain.response.Transaction
import java.math.BigInteger

class EthereumRetryableMono(
    private val monoEthereum: MonoEthereum,
    retryPolicy: ClientRetryPolicyProperties
) : AbstractRetryableClient(retryPolicy) {

    fun ethBlockNumber(): Mono<BigInteger> {
        return monoEthereum.ethBlockNumber()
            .wrapWithRetry("ethBlockNumber")
    }

    fun ethGetFullBlockByNumber(number: BigInteger): Mono<Block<Transaction>> {
        return monoEthereum.ethGetFullBlockByNumber(number)
            .wrapWithRetry("ethGetFullBlockByNumber", number)
    }

    fun ethGetLogsJava(filter: LogFilter): Mono<List<Log>> {
        return monoEthereum.ethGetLogsJava(filter)
            .wrapWithRetry("ethGetLogsJava", filter)
    }

    fun ethGetFullBlockByHash(hash: Word): Mono<Block<Transaction>> {
        return monoEthereum.ethGetFullBlockByHash(hash)
            .wrapWithRetry("ethGetFullBlockByHash", hash)
    }
}