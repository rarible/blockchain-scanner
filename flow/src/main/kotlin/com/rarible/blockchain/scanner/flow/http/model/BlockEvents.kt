package com.rarible.blockchain.scanner.flow.http.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

data class BlockEvents(
    @JsonProperty("block_id")
    val blockId: String,
    @JsonProperty("block_height")
    val blockHeight: String,
    @JsonProperty("block_timestamp")
    val blockTimestamp: Instant,
    val events: List<Event>
)