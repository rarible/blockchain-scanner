package com.rarible.blockchain.scanner.solana.client

import com.rarible.blockchain.scanner.solana.client.dto.SolanaAccountInfoDto

class SolanaMetaClient(
    private val api: SolanaApi,
) {
    suspend fun getMetadata(address: String): SolanaAccountInfoDto.Metadata? {
        return api.getAccountInfo(address).result?.value?.data?.parsed?.info?.extensions
            ?.firstOrNull { it.extension == "tokenMetadata" }?.toMetadata()
    }
}
