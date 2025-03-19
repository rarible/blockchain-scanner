package com.rarible.blockchain.scanner.reindex

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

interface ReindexParam {

    companion object {
        const val BLOCK_SCANNER_REINDEX_TASK = "BLOCK_SCANNER_REINDEX_TASK"
        private val mapper = ObjectMapper().registerModules().registerKotlinModule()

        fun <T> parse(json: String, type: Class<T>): T {
            return mapper.readValue(json, type)
        }
    }

    val range: BlockRange
    val name: String?
    val publishEvents: Boolean

    fun toJson(): String = mapper.writeValueAsString(this)

    fun <T> copyWithRange(range: BlockRange): T
}
