package com.rarible.blockchain.scanner.handler

import org.slf4j.LoggerFactory

class BlockHandlerException : RuntimeException {
    constructor(message: String, cause: Throwable): super(message, cause)
}

private val logger = LoggerFactory.getLogger("BlockHandlerException")

suspend fun <T> runRethrowingBlockHandlerException(actionName: String, block: suspend () -> T): T {
    return try {
        block()
    } catch (e: Exception) {
        logger.error("Failed to '$actionName'", e)
        if (e is BlockHandlerException) {
            throw e
        }
        throw BlockHandlerException("Failed to '$actionName'", e)
    }
}
