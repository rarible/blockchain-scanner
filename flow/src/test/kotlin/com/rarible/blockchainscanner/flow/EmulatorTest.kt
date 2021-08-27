package com.rarible.blockchainscanner.flow

import com.nftco.flow.sdk.*
import com.nftco.flow.sdk.cadence.ArrayField
import com.nftco.flow.sdk.cadence.StringField
import com.nftco.flow.sdk.crypto.Crypto
import com.rarible.core.test.containers.KGenericContainer
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile

@ExperimentalCoroutinesApi
@Testcontainers
internal class EmulatorTest {

    private var userPrivateKeyHex: String = ""
    private var userPublicKeyHex: String = ""
    private lateinit var accessApi: FlowAccessApi
    private val latestBlockID: FlowId get() = accessApi.getLatestBlockHeader().id


    companion object {

        private const val GRPC_PORT = 3569

        val serviceAccountAddress: FlowAddress = FlowAddress("f8d6e0586b0a20c7")

        @Container
        private val flowEmulator: KGenericContainer = KGenericContainer(
            "zolt85/flow-cli-emulator:27"
        ).withEnv("FLOW_BLOCKTIME", "500ms")
            .withCopyFileToContainer(MountableFile.forClasspathResource("com/rarible/blockchainscanner/flow/contracts"), "/home/flow/contracts")
            .withCopyFileToContainer(MountableFile.forClasspathResource("com/rarible/blockchainscanner/flow/flow.json"), "/home/flow/flow.json")
            .withExposedPorts(GRPC_PORT, 8080)
            .withLogConsumer {
                println(it.utf8String)
            }
            .waitingFor(Wait.forHttp("/").forPort(8080).forStatusCode(500))

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            println(flowEmulator.execInContainer("flow", "project", "deploy"))
        }
    }



    @BeforeEach
    internal fun setUp() {
        val pair = Crypto.generateKeyPair()
        userPrivateKeyHex = pair.private.hex
        userPublicKeyHex = pair.public.hex
        accessApi = Flow.newAccessApi(flowEmulator.host, flowEmulator.getMappedPort(GRPC_PORT))
    }

    @Test
    internal fun `check accounts test`() {
        val expected = EmulatorUser.values().filterNot { it.name == "Emulator" }.map { it.address }.toTypedArray()
        val script = FlowScript(
            script = """
                pub fun main(): [Address] {
                    let patric = getAccount(${EmulatorUser.Patrick.address.formatted})
                    let squidward = getAccount(${EmulatorUser.Squidward.address.formatted})
                    let gary = getAccount(${EmulatorUser.Gary.address.formatted})
                    return [patric.address, squidward.address, gary.address]
                }
            """.trimIndent()
        )
        val scriptResult = accessApi.executeScriptAtLatestBlock(script)
        Assertions.assertNotNull(scriptResult)
        Assertions.assertTrue(scriptResult.jsonCadence is ArrayField)
        val data = scriptResult.jsonCadence as ArrayField
        Assertions.assertArrayEquals(expected, data.value!!.map { FlowAddress(it.value.toString()) }.toTypedArray())
    }

    @Test
    internal fun helloWorldTest() {
        val payerAccountKey = getAccountKey(serviceAccountAddress, 0)

        var tx = FlowTransaction(
            script = FlowScript("""
                import HelloWorld from 0xf8d6e0586b0a20c7

                transaction {

                  prepare(acct: AuthAccount) {}

                  execute {
                    log(HelloWorld.hello())
                  }
                }
            """.trimIndent()),
            arguments = emptyList(),
            referenceBlockId = latestBlockID,
            gasLimit = 100L,
            proposalKey = FlowTransactionProposalKey(
                address = serviceAccountAddress,
                keyIndex = payerAccountKey.id,
                sequenceNumber = payerAccountKey.sequenceNumber.toLong()
            ),
            payerAddress = serviceAccountAddress,
            authorizers = listOf(serviceAccountAddress)
        )

        val signer = Crypto.getSigner(privateKey = privateKey(), hashAlgo = payerAccountKey.hashAlgo)
        tx = tx.addEnvelopeSignature(serviceAccountAddress, payerAccountKey.id, signer)
        val txId = accessApi.sendTransaction(tx)
        val txResult = waitForSeal(api = accessApi, transactionId = txId)
        Assertions.assertNotNull(txResult)
        Assertions.assertTrue(txResult.errorMessage.isEmpty())
        println(txResult)

        val acc = getAccount(serviceAccountAddress)
        Assertions.assertTrue(acc.contracts.containsKey("HelloWorld"))
    }

    @Test
    internal fun createAccountTest() {
        Assertions.assertTrue(flowEmulator.isRunning)
        val account = createAccount(serviceAccountAddress, userPublicKeyHex)
        Assertions.assertNotNull(account)
    }

    private fun createAccount(payer: FlowAddress, publicKeyHex: String): FlowAddress {
        val payerAccountKey = getAccountKey(payer, 0)

        val newAccountPublicKey = FlowAccountKey(
            publicKey = FlowPublicKey(publicKeyHex),
            signAlgo = SignatureAlgorithm.ECDSA_P256,
            hashAlgo = HashAlgorithm.SHA3_256,
            weight = 1000
        )

        var tx = FlowTransaction(
            script = FlowScript(
                """
                transaction(publicKey: String) {
                	prepare(signer: AuthAccount) {
                		let account = AuthAccount(payer: signer)
                        account.addPublicKey(publicKey.decodeHex())
                	}
                }
            """.trimIndent()
            ),
            arguments = listOf(
                FlowArgument(StringField(newAccountPublicKey.encoded.bytesToHex()))
            ),
            referenceBlockId = latestBlockID,
            gasLimit = 100L,
            proposalKey = FlowTransactionProposalKey(
                address = payer,
                keyIndex = payerAccountKey.id,
                sequenceNumber = payerAccountKey.sequenceNumber.toLong()
            ),
            payerAddress = payer,
            authorizers = listOf(payer)
            )


        val signer = Crypto.getSigner(privateKey = privateKey(), hashAlgo = payerAccountKey.hashAlgo)
        tx = tx.addEnvelopeSignature(payer, payerAccountKey.id, signer)
        val txId = accessApi.sendTransaction(tx)
        val txResult = waitForSeal(api = accessApi, transactionId = txId)
        return getAccountCreatedAddress(txResult)
    }

    private fun privateKey() =
        Crypto.decodePrivateKey("8ea7b6cb8da7a09c19e2401eafcfd3863136decb5a495779a22f917c376da8b4")

    private fun getAccountCreatedAddress(txResult: FlowTransactionResult): FlowAddress {
        val addressHex = txResult.events[0]
            .event
            .value!!
            .fields[0]
            .value.value as String
        return FlowAddress(addressHex.substring(2))
    }

    private fun getAccountKey(address: FlowAddress, keyIndex: Int): FlowAccountKey {
        val account = getAccount(address)
        return account.keys[keyIndex]
    }

    private fun getAccount(address: FlowAddress): FlowAccount = accessApi.getAccountAtLatestBlock(address)!!
}
