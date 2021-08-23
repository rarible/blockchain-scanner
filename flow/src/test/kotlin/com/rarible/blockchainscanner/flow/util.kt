package com.rarible.blockchainscanner.flow

import com.nftco.flow.sdk.FlowAddress

sealed class AccInfo(val name: String, val address: FlowAddress, val keyHex: String, val pubHex: String)


class Patrick: AccInfo(
    name = "Patrick",
    address = FlowAddress("3e63575a95c82b4a"),
    keyHex = "71f64f0287a49382b1476323f363563bc32d8cdd8d81ba335e0b76f14555e5d2",
    pubHex = "08bdf0842d8405f0215259694cbb6cd44f6a6320976921be12c2609d374d456835fa9c73aa6e6bdefffbea807451e3230417bc25dea96c04d53af86cbdd0bb9f"
)

class Squidward: AccInfo(
    name = "Squidward",
    address = FlowAddress("b53b9ed5e3908303"),
    keyHex = "5bd89c2dbacd509e79065e65be99655946cdb339e912efc1e55e02254939e704",
    pubHex = "dfd1bdb38fc8b7b397edc7058564edc07760a0760c7e8bfff819f8d9763f0e73c5755d8ab5c367714949c9e7ce0015948b931bdb7fc2591fd215bc75908d03cd"
)

class Gary: AccInfo(
    name = "Gary",
    address = FlowAddress("b2d6df637534cf3f"),
    keyHex = "6fcee83ae408d909aaf73a88c8a99ace0af5a77aa2cd3990b7ead55fc097e52c",
    pubHex = "74c3da5436374b0fd36bbe6e8d2c0a1e4437a029e6ac793d2c1d9b6f352fd1fa2e7fcb5b6b3851f21604fc092c2135916d2931cd2c641f063d603c687ec390dd"
)
