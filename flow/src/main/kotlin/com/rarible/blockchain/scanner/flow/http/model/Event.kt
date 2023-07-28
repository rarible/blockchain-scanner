package com.rarible.blockchain.scanner.flow.http.model

import com.fasterxml.jackson.annotation.JsonProperty

data class Event(
    val type: String,
    @JsonProperty("transaction_id")
    val transactionId: String,
    @JsonProperty("transaction_index")
    val transactionIndex: String,
    @JsonProperty("event_index")
    val eventIndex: String,
    val payload: String
)
