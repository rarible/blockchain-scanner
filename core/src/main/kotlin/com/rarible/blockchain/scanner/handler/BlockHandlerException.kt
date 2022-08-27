package com.rarible.blockchain.scanner.handler

import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("BlockHandlerException")

suspend fun <T> runRethrowingBlockHandlerException(actionName: String, block: suspend () -> T): T {
    return try {
        block()
    } catch (e: Exception) {
        logger.error("Failed to '$actionName'", e)
        throw e
    }
}
