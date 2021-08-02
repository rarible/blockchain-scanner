package com.rarible.blockchain.scanner.framework.mapper

import com.rarible.blockchain.scanner.framework.client.BlockchainBlock
import com.rarible.blockchain.scanner.framework.model.Block

//todo давай на всех (ну или почти) классах и интерфейсах общих (из этого модуля) буквально одно предложение напишем в javadoc -
//todo зачем нужен этот класс, какую роль выполняет. с ходу не всегда понятно
interface BlockMapper<BB : BlockchainBlock, B : Block> {

    fun map(originalBlock: BB): B

}