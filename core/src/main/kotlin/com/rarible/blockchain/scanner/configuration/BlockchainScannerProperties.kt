package com.rarible.blockchain.scanner.configuration

interface BlockchainScannerProperties {

    //@Value("\${ethereumMaxProcessTime:300000}")
    val maxProcessTime: Long

    //@Value("\${ethereumBackoffMaxAttempts:5}")
    val maxAttempts: Long

    //@Value("\${ethereumBackoffMinBackoff:100}")
    val minBackoff: Long

    //@Value("\${ethereumBlockBatchSize:100}")
    val batchSize: Long

    val reindexEnabled: Boolean

}