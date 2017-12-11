package org.zuinnote.spark.ethereum

import org.zuinnote.spark.ethereum.model._
import org.zuinnote.spark._

object TestData {
  lazy val block1346406 = EthereumBlock(
    EthereumBlockHeader(
      "ba6dd26012b3719048f316c6edb3349bdfbd61319fa97c616a613118a1af3067".bytes,
      "1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347".bytes,
      "1a060b0604883a99809eb3f798df71bef6c358f1".bytes,
      "21ba886fd26f17b401f53920153310b6939bad8a5fc3bf8c505c556ddbafbc5c".bytes,
      "b3cbc7f0d787e57d9370b802ab945e21991c3e127d70120c37e9fdae3ef3ebfc".bytes,
      "9bce7132f52d4d45a8a247484786c70bb2e63959c8561b3abfd4e722e6006a27".bytes,
      "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000".bytes,
      "19ff9ec435e0".bytes,
      1460799269L,
      1346406L,
      4712388L,
      126000L,
      "4f5771b79a8e6e219935539c473e23bafd2ca35cc186206631c3b09ed576194a".bytes,
      "d783010305844765746887676f312e352e31856c696e7578".bytes,
      "ff7c7aee0e88c52d".bytes
    ),
    Seq(
      EthereumTransaction(
        "0c".bytes,
        1069000990000000000L,
        "1e75f02a6e9ff4ff16333825d909bb033306b78b".bytes,
        20000000000L,
        21000L,
        "".bytes,
        "1b".bytes,
        "47ddf9376897767813955a9d46b6f1aa7773e5c8c62167541c80bf252dc7dcd2".bytes,
        "0a313f3560323756b7285f6238518605821a2bee037dea8f0922662089037459".bytes
      ),
      EthereumTransaction(
        "ffd7".bytes,
        5110508700000000000L,
        "5467fabd30eb61a18461d153d8c6ffb19dd47a25".bytes,
        20000000000L,
        90000L,
        "".bytes,
        "1b".bytes,
        "62853c639a9b9ec49ba9ac53e285b34ed0b7655c1be329fb8b3470740c3d0a9a".bytes,
        "03faa6f4ff1a4576df089a9f9cb79cf2edc1c5bdec0fe79c792acb9e83f241".bytes
      ),
      EthereumTransaction(
        "02d7dd".bytes,
        11667800000000000L,
        "b4d0ca2b7e4cb1e0610d02154a10163ab0f42e65".bytes,
        20000000000L,
        90000L,
        "".bytes,
        "1c".bytes,
        "89fd7a62cf4477bfe5dbf0eecf3a4a96719696fbbe16ba0aba1d631d44c1eb58".bytes,
        "24344864eb6a60c66fb5daed02b56352e8174216b8a2d333b7f332ff6ba0699c".bytes
      ),
      EthereumTransaction(
        "02d7de".bytes,
        130970170000000000L,
        "1f57f826caf594f7a837d9fc092456870a289365".bytes,
        20000000000L,
        90000L,
        "".bytes,
        "1b".bytes,
        "460157dce4e95d1dcc7aed0d9b7e3d65370c53d29ea9b1aa4c9c2214911cd95e".bytes,
        "6a844f956d0246941b943091342120bd48e7c63577f0ba3d8759c9ec58704eec".bytes
      ),
      EthereumTransaction(
        "02d7df".bytes,
        144683800000000000L,
        "1f57f826caf594f7a837d9fc092456870a289365".bytes,
        20000000000L,
        90000L,
        "".bytes,
        "1c".bytes,
        "e4be97d5aff1b5e7991296982bdfc1c22f7521134f7e1a9da300420dad336f34".bytes,
        "62def8aa836558c7b0a565b97c9b27b20ed9a051de22ad8dbd625244ce649e3d".bytes
      ),
      EthereumTransaction(
        "02d7e0".bytes,
        143694920000000000L,
        "1f57f826caf594f7a837d9fc092456870a289365".bytes,
        20000000000L,
        90000L,
        "".bytes,
        "1c".bytes,
        "0a4ca218460dc85b990746fbb90c06f8258782808727983c8b8d6a921e199bca".bytes,
        "08a3b9a45d831ac4ad379d14f0ae3c03c8731cb44d8a79acd4cd6cea1b548002".bytes
      )
    ),
    Seq.empty
  )

  lazy val block1346406EnrichedTransactionData = Seq(
    EnrichedTransactionAdditions(
      "39424bd28a2223da3e14bf793cf7f8208ee9980a".bytes,
      "e27e9288e29cc8eb78f9f768d89bf1cd4b68b715a38b95d46d778618cb104d58".bytes
    ),
    EnrichedTransactionAdditions(
      "4bb96091ee9d802ed039c4d1a5f6216f90f81b01".bytes,
      "7a232aa2ae6a5e1f32ca3ac93f4fdb77983e932b38099356444208c69d408681".bytes
    ),
    EnrichedTransactionAdditions(
      "63a9975ba31b0b9626b34300f7f627147df1f526".bytes,
      "1433e3cb662f668d87b83555345a20ccf8706f25214918e2f81fe3d21c9d5b23".bytes
    ),
    EnrichedTransactionAdditions(
      "63a9975ba31b0b9626b34300f7f627147df1f526".bytes,
      "3922f7f60a33a12d139d67fa5330dbfdba42a4b767296eff6415eea32d8a7b2b".bytes
    ),
    EnrichedTransactionAdditions(
      "63a9975ba31b0b9626b34300f7f627147df1f526".bytes,
      "bb7caa23385a0f73753f9e28d8f0602fe2e72d87e1e095527528d144885d6b51".bytes
    ),
    EnrichedTransactionAdditions(
      "63a9975ba31b0b9626b34300f7f627147df1f526".bytes,
      "bcde6f49842c6d738d64328f7809b1d49bf0ff3ffa460fddd27fd42b7a01fc9a".bytes
    )
  )
}

final case class EnrichedTransactionAdditions(sendAddress: Seq[Byte], hash: Seq[Byte])