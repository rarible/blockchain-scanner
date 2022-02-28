package com.rarible.blockchain.scanner.ethereum.configuration

data class TaskProperties(
    val runReindexTask: Boolean = false,
    val runCheckTask: Boolean = false
)