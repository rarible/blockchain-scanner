package com.rarible.blockchain.scanner.flow

import com.nftco.flow.sdk.Flow
import com.nftco.flow.sdk.FlowAccessApi
import com.nftco.flow.sdk.FlowAccount
import com.nftco.flow.sdk.FlowAccountKey
import com.nftco.flow.sdk.FlowAddress
import com.nftco.flow.sdk.FlowId
import com.nftco.flow.sdk.FlowScript
import com.nftco.flow.sdk.FlowTransaction
import com.nftco.flow.sdk.FlowTransactionProposalKey
import com.nftco.flow.sdk.crypto.Crypto
import com.nftco.flow.sdk.waitForSeal
import com.rarible.blockchain.scanner.flow.model.FlowLogRecord
import com.rarible.blockchain.scanner.flow.repository.FlowLogRepository
import com.rarible.blockchain.scanner.flow.subscriber.FlowLogEventSubscriber
import com.rarible.blockchain.scanner.flow.test.TestFlowScannerConfiguration
import com.rarible.core.test.containers.KGenericContainer
import com.rarible.core.test.ext.MongoCleanup
import com.rarible.core.test.ext.MongoTest
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ContextConfiguration
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile
import java.time.Duration

@SpringBootTest(
    properties = [
        "application.environment = test",
        "spring.application.name = test-flow-scanner",
        "spring.cloud.consul.config.enabled = false",
        "spring.cloud.service-registry.auto-registration.enabled = false",
        "spring.cloud.discovery.enabled = false",
        "logging.logstash.tcp-socket.enabled = false",
        "logging.logjson.enabled = false",
        "rarible.task.initialDelay=0",
        "blockchain.scanner.flow.chainId=EMULATOR",
        "blockchain.scanner.flow.poller.delay=200",
    ]
)
@ContextConfiguration(classes = [TestFlowScannerConfiguration::class])
@MongoCleanup
@MongoTest
@Testcontainers
@Disabled
class FlowScannerTest {

    private lateinit var accessApi: FlowAccessApi
    private val latestBlockID: FlowId get() = accessApi.getLatestBlockHeader().id

    @Autowired
    private lateinit var logRepository: FlowLogRepository

    @Autowired
    private lateinit var allFlowLogEventSubscriber: FlowLogEventSubscriber

    companion object {
        @Container
        private val flowEmulator: KGenericContainer = KGenericContainer(
            "zolt85/flow-cli-emulator:27"
        ).withEnv("FLOW_BLOCKTIME", "300ms")
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("com/rarible/blockchain/scanner/flow/contracts"),
                "/home/flow/contracts"
            )
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("com/rarible/blockchain/scanner/flow/flow.json"),
                "/home/flow/flow.json"
            )
            .withExposedPorts(3569, 8080)
            .withLogConsumer {
                println(it.utf8String)
            }
            .withReuse(true)
            .waitingFor(Wait.forHttp("/").forPort(8080).forStatusCode(500))

        @BeforeAll
        @JvmStatic
        internal fun setup() {
            println(flowEmulator.execInContainer("flow", "project", "deploy").stdout)
            println(
                flowEmulator.execInContainer(
                    "flow",
                    "accounts",
                    "create",
                    "--key",
                    EmulatorUser.Patrick.pubHex
                ).stdout
            )
            println(
                flowEmulator.execInContainer(
                    "flow",
                    "accounts",
                    "create",
                    "--key",
                    EmulatorUser.Squidward.pubHex
                ).stdout
            )
            println(
                flowEmulator.execInContainer(
                    "flow",
                    "accounts",
                    "create",
                    "--key",
                    EmulatorUser.Gary.pubHex
                ).stdout
            )
        }
    }

    @BeforeEach
    internal fun setUp() {
        accessApi = Flow.newAccessApi(host = flowEmulator.host, port = flowEmulator.getMappedPort(3569))
    }

    @Test
    internal fun `scanner test`() = runBlocking {
        var proposalKey = getAccountKey(EmulatorUser.Emulator.address)
        var authKey = getAccountKey(EmulatorUser.Patrick.address)
        var initTx = FlowTransaction(
            script = FlowScript(
                """
                import ExampleNFT from ${EmulatorUser.Emulator.address.formatted}

                // This transaction configures a user's account
                // to use the NFT contract by creating a new empty collection,
                // storing it in their account storage, and publishing a capability
                transaction {
                    prepare(acct: AuthAccount) {

                        // Create a new empty collection
                        let collection <- ExampleNFT.createEmptyCollection()

                        // store the empty NFT Collection in account storage
                        acct.save<@ExampleNFT.Collection>(<-collection, to: /storage/NFTCollection)

                        log("Collection created for account 1")

                        // create a public capability for the Collection
                        acct.link<&{ExampleNFT.NFTReceiver}>(/public/NFTReceiver, target: /storage/NFTCollection)

                        log("Capability created")
                    }
                }
            """.trimIndent()
            ),
            arguments = listOf(),
            referenceBlockId = latestBlockID,
            gasLimit = 100L,
            payerAddress = EmulatorUser.Emulator.address,
            proposalKey = FlowTransactionProposalKey(
                address = EmulatorUser.Emulator.address,
                keyIndex = proposalKey.id,
                sequenceNumber = proposalKey.sequenceNumber.toLong()
            ),
            authorizers = listOf(EmulatorUser.Patrick.address),
        )

        val payerSigner = Crypto.getSigner(
            privateKey = Crypto.decodePrivateKey(EmulatorUser.Emulator.keyHex),
            hashAlgo = proposalKey.hashAlgo
        )
        val authSigner = Crypto.getSigner(
            privateKey = Crypto.decodePrivateKey(EmulatorUser.Patrick.keyHex),
            hashAlgo = authKey.hashAlgo
        )
        initTx = initTx.addPayloadSignature(
            address = EmulatorUser.Patrick.address,
            keyIndex = authKey.id,
            signer = authSigner
        )
        initTx = initTx.addEnvelopeSignature(
            address = EmulatorUser.Emulator.address,
            keyIndex = proposalKey.id,
            signer = payerSigner
        )

        var txId = accessApi.sendTransaction(initTx)
        val txResult = waitForSeal(api = accessApi, transactionId = txId)
        println("txResult: $txResult")
        Assertions.assertNotNull(txResult)

        proposalKey = getAccountKey(EmulatorUser.Emulator.address)
        authKey = getAccountKey(EmulatorUser.Patrick.address)

        var mintTx = FlowTransaction(
            script = FlowScript(
                """
                // Transaction2.cdc

                import ExampleNFT from ${EmulatorUser.Emulator.address.formatted}

                // This transaction allows the Minter account to mint an NFT
                // and deposit it into its collection.

                transaction {

                    // The reference to the collection that will be receiving the NFT
                    let receiverRef: &{ExampleNFT.NFTReceiver}

                    // The reference to the Minter resource stored in account storage
                    let minterRef: &ExampleNFT.NFTMinter

                    prepare(acct: AuthAccount, minterAcct: AuthAccount) {
                        // Get the owner's collection capability and borrow a reference
                        self.receiverRef = acct.getCapability<&{ExampleNFT.NFTReceiver}>(/public/NFTReceiver)
                            .borrow()
                            ?? panic("Could not borrow receiver reference")

                        // Borrow a capability for the NFTMinter in storage
                        self.minterRef = minterAcct.borrow<&ExampleNFT.NFTMinter>(from: /storage/NFTMinter)
                            ?? panic("could not borrow minter reference")
                    }

                    execute {
                        // Use the minter reference to mint an NFT, which deposits
                        // the NFT into the collection that is sent as a parameter.
                        let newNFT <- self.minterRef.mintNFT()

                        self.receiverRef.deposit(token: <-newNFT)

                        log("NFT Minted and deposited to Account 2's Collection")
                    }
                }
            """.trimIndent()
            ),
            arguments = listOf(),
            referenceBlockId = latestBlockID,
            gasLimit = 100L,
            proposalKey = FlowTransactionProposalKey(
                address = EmulatorUser.Emulator.address,
                keyIndex = proposalKey.id,
                sequenceNumber = proposalKey.sequenceNumber.toLong()
            ),
            payerAddress = EmulatorUser.Emulator.address,
            authorizers = listOf(EmulatorUser.Patrick.address, EmulatorUser.Emulator.address)
        )

        mintTx = mintTx.addPayloadSignature(
            address = EmulatorUser.Patrick.address,
            keyIndex = authKey.id,
            signer = authSigner
        )
        mintTx = mintTx.addEnvelopeSignature(
            address = EmulatorUser.Emulator.address,
            keyIndex = proposalKey.id,
            signer = payerSigner
        )

        txId = accessApi.sendTransaction(mintTx)
        val event = awaitMintEvent()
        Assertions.assertEquals(txId, FlowId(event.log.transactionHash), "Transactions are not equals")
    }

    private suspend fun awaitMintEvent(): FlowLogRecord {
        var founded: FlowLogRecord? = null
        Assertions.assertTimeout(Duration.ofSeconds(10L)) {
            runBlocking {
                while (founded == null) {
                    founded = try {
                        val descriptor = allFlowLogEventSubscriber.getDescriptor()
                        logRepository.findByLogEventType(
                            entityType = descriptor.entityType,
                            collection = descriptor.collection,
                            eventType = "A.f8d6e0586b0a20c7.ExampleNFT.Mint"
                        )
                    } catch (e: Exception) {
                        when (e) {
                            is IllegalArgumentException -> Assertions.fail("More than one mint log founded!")
                            else -> null
                        }
                    }
                }
            }
        }
        return founded!!
    }

    private fun getAccountKey(address: FlowAddress, keyIndex: Int = 0): FlowAccountKey {
        val account = getAccount(address)
        return account.keys[keyIndex]
    }

    private fun getAccount(address: FlowAddress): FlowAccount = accessApi.getAccountAtLatestBlock(address)!!
}
