package com.rarible.blockchain.scanner.ethereum.model

import com.rarible.blockchain.scanner.framework.model.LogRecord
import org.bson.types.ObjectId
import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Version

abstract class EthereumLogRecord(

    @Id
    var id: ObjectId,

    @Version
    var version: Long?,

    override var log: EthereumLog? = null

) : LogRecord<EthereumLog>