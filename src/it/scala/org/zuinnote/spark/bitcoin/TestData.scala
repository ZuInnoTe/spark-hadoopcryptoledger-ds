package org.zuinnote.spark.bitcoin

import org.zuinnote.spark._
import org.zuinnote.spark.bitcoin.model._

object TestData {
  private lazy val  expectedGenesisTransaction: Transaction = Transaction(
    1, 1, 0, "01".bytes, "01".bytes,
    Seq(Input("0000000000000000000000000000000000000000000000000000000000000000".bytes, 4294967295L, "4d".bytes, "04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73".bytes, 4294967295L)),
    Seq(Output(5000000000L, "43".bytes, "4104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac".bytes)),
    Seq.empty, 0
  )

  lazy val expectedGenesisBlock: BitcoinBlock = BitcoinBlock(
    285, "f9beb4d9".bytes, 1, 1231006505, "ffff001d".bytes, 2083236893, 1L,
    "0000000000000000000000000000000000000000000000000000000000000000".bytes,
    "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a".bytes,
    Seq(
      expectedGenesisTransaction
    )
  )

  lazy val expectedEnrichedGenesisBlock: EnrichedBitcoinBlock = expectedGenesisBlock.enriched(
    Seq(expectedGenesisTransaction.enriched("3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a".bytes))
  )

  lazy val expectedNamecoinBlock: BitcoinBlock = {
    val transaction1 = Transaction(
      1, 1, 0, "01".bytes, "01".bytes,
      Seq(Input("0000000000000000000000000000000000000000000000000000000000000000".bytes, 4294967295L, "06".bytes, "03ef8a050107".bytes, 4294967295L)),
      Seq(Output(2501615481L, "23".bytes, "2102b7436f66cc25a4291edd23a545fab88a2b0785be2bd9ad5911edd0afdbd8aa56ac".bytes)),
      Seq.empty, 0
    )

    val transaction2 = Transaction(
      1, 1, 0, "02".bytes, "02".bytes,
      Seq(
        Input("872c31f158ab52268477b81c20dda8899a8bd83e764676c772273e31920a4603".bytes, 1L, "6b".bytes, "483045022100d3e4691ca734573d9b961c051a0a5f912a4241ca6fc0bc46f26f11c8b3c35eeb0220726a288cb4876b4c93ea235e6badfd8126ae286ab1e7572da86e5d46e49eceaa012103fc85e2c3cec63f27e29606c8d4a011f7cc401ba448bf1b5cdb6eb6b2dec25cfa".bytes, 4294967295L),
        Input("8eec0db87372f19834d2eb1d53ae015662d8ae611b0cef45a91db0fe0a4c39da".bytes, 1L, "6a".bytes, "4730440220690426333140fbffdcd6dc707b0f884547dec4d8c379b656ebdc31a85292d6d502207037a944c7b9a4de855b60ef0147248d3ead2db6cf47461698db1b52434ceed4012103cd58870f20e5bdb59fd5cba287e234618d1a2c2d5afd4613a93be752fb624ec9".bytes, 4294967295L)
      ),
      Seq(
        Output(100219014810L, "19".bytes, "76a9144b9175157fd919a67d462494580efa8f0201f3a288ac".bytes),
        Output(28999089554L, "19".bytes, "76a9145efadc8054131cade88480e060c7f4784a70e7f488ac".bytes)
      ),
      Seq.empty, 0
    )

    val transaction3 = Transaction(
      1, 1, 0, "03".bytes, "02".bytes,
      Seq(
        Input("ad1fa5ee9c97ae843691e5800fef87d6270700b2178478bf47025def8284f2aa".bytes, 1L, "6b".bytes, "483045022100d3c8a1803bfb09dc0621af95a07c6449bb78520a2d501fc2d851ee7c22d31dc5022049ad5e0ff170f9b9742f1403d5161ade98fff71bbcb1b84b82eea264d09b19b10121023fd585ce62d21fef4d0cc48edaad64b56520d93c041260e9055720d4a46a7331".bytes, 4294967295L),
        Input("6b9d656661df438ff8f46b456e6d7e89db57e52b87a0ae2688f56fe0734dfe29".bytes, 1L, "6a".bytes, "47304402205df4351ecd7ea983cd9bb3132354d74798759df1d09942f870643debc46f77cb022024f40d246afceeb4245b5c578923cb9b42aca9b37f89cf449bb1df93889536ce012102d86b058f199becbb8619be168bab4c8cd1f21ba9c6df5d6e1d864b81eedaf9bc".bytes, 4294967295L),
        Input("ad2470877788c5b968da402e77179f0fb54e9c4a4927452cec091bbc286c5ac6".bytes, 1L, "6b".bytes, "483045022100d43a8a31238ad1bf3b348dd9f6234541095afab4153a6bba8c42e1ee7612d23202203285c7f0fce84390cbbbe094e6ac2a68b8626b3630785393ef05c23eb8af2a29012103521ed616d5594366680a14f7eddc3c2953bb672f25de6f9a480613f02beef03a".bytes, 4294967295L)
      ),
      Seq(
        Output(26399500000L, "19".bytes, "76a914daa7c971e5eeb1cb8e0bbf857462a8be0526a51d88ac".bytes),
        Output(63599500000L, "19".bytes, "76a9149043037b1bcb5c277420d2f6e36c87d527fd052688ac".bytes)
      ),
      Seq.empty, 0
    )

    val transaction4 = Transaction(
      1, 1, 0, "01".bytes, "01".bytes,
      Seq(Input("87149cd48023a61a197f251f66219909868fd4d70d55971d46da876a489ec8d7".bytes, 0L, "6a".bytes, "47304402206f45b99286b54021a9338dde2d35faba910638a3b6060546053c5ddc139a72f902207854de83c8c835a4aeae2c46b0c1532eaadbcaa68d9148eb08e48b12f2bcce89012102d7efe9e993fe2cf63ccfecc048fc96c341a9d1e5c47adcefec999bb411829171".bytes, 4294967295L)),
      Seq(Output(26399000000L, "19".bytes, "76a914dc3b5c1204604461fc487602020bd25e889af62288ac".bytes)),
      Seq.empty, 0
    )

    val transaction5 = Transaction(
      28928, 1, 0, "01".bytes, "02".bytes,
      Seq(Input("fafd115b5191023cdda81ecddaa4a6dac114863ac5cf6d434dcf344ed1aa65c4".bytes, 0L, "6a".bytes, "4730440220586700aa759032014f99eb027f466142d75d7eafff9412fe106c16d224fe28a002204a725f893c9ed7e4b7ce10920275b64a0fff5af1b680d1ae96c1fcd770b3b144012103ff203940525497cbfc82b88de8134c13ec285ce85785b12680ddb97cd692aa57".bytes, 4294967294L)),
      Seq(
        Output(88775100L, "19".bytes, "76a914817fe39dabec93bdb513ad64cf66824c2f3fae9b88ac".bytes),
        Output(1000000L, "30".bytes, "511459c39a7cc5e0b91801294a272ad558b1f67a4e6d6d76a914dd900a6c1223698fc262e28c8a1d8d73b40b375188ac".bytes)
      ),
      Seq.empty, 363246
    )

    val transaction6 = Transaction(
      28928, 1, 0, "02".bytes, "02".bytes,
      Seq(
        Input("9fc8011ee2f79689dc995335948cd4e6a939fbb3a0fa202aea4afbc3369a1091".bytes, 0L, "6b".bytes, "483045022100b51e21c9452b416dc7e3b236126db699ac6bf68184de2acac6807eb5d4b39b5502206217f2d5c33816ce6f8702e0715e862cd12d17a10bda5ca4cf23d92107ba4704012103a759e09173ce2a6098401196c79a74656bffcdb760543d1921e1abac0b1d5e8a".bytes, 4294967294L),
        Input("9fc8011ee2f79689dc995335948cd4e6a939fbb3a0fa202aea4afbc3369a1091".bytes, 1L, "6b".bytes, "483045022100ee9d11543ffc698eb634465c13020f7574ad63e60f5557509370360d0182b23f02201e5a4c370eea6701e137f68836f7096010395b77eb20472e3c99b6340ba07260012103cfb64a90d8a00cc835c13a5262cb4d24506abbd4b9263291f22b4a2ec6913390".bytes, 4294967294L)
      ),
      Seq(
        Output(1315571753L, "19".bytes, "76a914207b1c0eaff7a741d5c991b7af7d13d233edc85088ac".bytes),
        Output(1000000L, "54".bytes, "5309642f70616e656c6b612d7b226970223a22382e382e382e38222c226d6170223a7b222a223a7b226970223a22382e382e382e38227d7d7d6d7576a9148d804b079ac79ad0ca108a4e5b679db591ff069b88ac".bytes)
      ),
      Seq.empty, 363216
    )

    val transaction7 = Transaction(
      28928, 1, 0, "02".bytes, "02".bytes,
      Seq(
        Input("da36c859561e70236493989653e7e66dd7f1b391cfca04c5c178f9c6cd915dc4".bytes, 0L, "6b".bytes, "4830450221009c3d9ea61e0e6d3c195fbf3ccd91387efc8ddbe21b6d8871c70c07b96500d941022020980e0c3b4704c7177d26eec303193f8d6c8a6bbeb19e17612c1a748b0c32280121022a2d993b22b23532395acaba67f66ac0aaaf78d78409bf1608a777a3901008b4".bytes, 4294967294L),
        Input("fb036a5e4fb1ca1e47e4e8f2f6a2adae454e75da728c2e9daa0d1f5ea85db148".bytes, 0L, "6a".bytes, "4730440220463191a27d5986577a2c0ac0038f49c3c0297521dee1d5fb392b717e418933d402201a95c48fcfa9354c060f8383f11a2dae4b9f23c3379f1d1ca45bf0271771c5f6012103005a1d2ff5f04ea5d0c155515209a69710ef37391b717ec4e6a8c7cbe4a37b23".bytes, 4294967294L)
      ),
      Seq(
        Output(1000000L, "7a".bytes, "520a642f666c6173687570641460c7b068edea60281daf424c38d8dab87c96cf993d7b226970223a223134352e3234392e3130362e323238222c226d6170223a7b222a223a7b226970223a223134352e3234392e3130362e323238227d7d7d6d6d76a91451b4fc93aab8cbdbd0ac9bc8eaf824643fc1e29b88ac".bytes),
        Output(1315524559L, "19".bytes, "76a9148a9813065e79fb5b97f4eb9f05d90a4cb802cfa888ac".bytes)
      ),
      Seq.empty, 363246
    )

    BitcoinBlock(
      3125, "f9beb4fe".bytes, 65796, 1506767051, "71630118".bytes, 0, 7L,
      "1c6278a700d98d38be4d34f4fd7382b1914413f984ff7c9307983c4b352a2d22".bytes,
      "777844d05a401c9b4937b2b8104450380f66f24096696bcbd8037b805ff191b7".bytes,
      Seq(transaction1, transaction2, transaction3, transaction4, transaction5, transaction6, transaction7)
    )
  }

  lazy val expectedNamecoinBlockWithAuxPOW: BitcoinBlockWithAuxPOW = {
    val auxPOW = AuxPOW(
      1,
      CoinbaseTransaction(1, "01".bytes, "02".bytes,
        Seq(Input("0000000000000000000000000000000000000000000000000000000000000000".bytes, 4294967295L, "45".bytes, "03fd7007fabe6d6d60510108f422a696be427a68c375bdafd4d32eaaa072ff7989211c4929f8f57401000000000000000165040026ba7cd7ee0000eb85052f736c7573682f".bytes, 0L)),
        Seq(
          Output(1440479672L, "19".bytes, "76a9147c154ed1dc59609e3d26abb2df2ea3d587cd8c4188ac".bytes),
          Output(0L, "26".bytes, "6a24aa21a9eddafd0475112ed1f9be3d33ec5344506955605889819b73d51e19e09dcaeff462".bytes)
        ),
        0
      ),
      "189f9ba054a01a4e4bf4ee2d4d97684f073c8ec2097129010000000000000000".bytes,
      CoinbaseBranch(
        "0c".bytes,
        Seq(
          "f16e09208dad8a34cc7311b50ccfb5739673898d5f25dd62adf2f9bd932b7664".bytes,
          "0869017be0e969fa368e5b0bd23cda5e916a455bccb30596591ca581271540f1".bytes,
          "9dffe883621d834384e49e1ec8c74fedc2e4302d45ce0782d0bfb346d8e3a23e".bytes,
          "a2a2a85506e8de78e3a011e332b48e3718562c6c5a014a19a07f44ad62b7aec0".bytes,
          "ac49061ca51df92ac1e96120341c756bc63984c9b855857b3fb6ccb89c1aa9ea".bytes,
          "b4d72fd26433a6e7954d3c44bba20193e2c546a7bdcf0699708419ab308c4c9e".bytes,
          "c71fac5b79b14e9b5c9a7c6c43d26b3c015a2d819dc8a2b7c5b84d45af790915".bytes,
          "9ee801aadfbc2ba20f924052044ebc83aced4bcd5e33ce378e9062bae3df674b".bytes,
          "3e8865342dd3a0ca30554ce47d2c8e9dddd792bf5523aa67a4d3cff735c67f55".bytes,
          "c6ed5da1a253d0bfdba7b19eca597a4a815fadce9f2230c5646700a8ed6e444c".bytes,
          "e2e05fecccae1085925029a78356b62a1e5cd3f68e2e364ab6cd4532a5c3f225".bytes,
          "18872ece228f90195ddc42434b31b04c33706d925605d235b8b3054aa573fafa".bytes
        ),
        "00000000".bytes
      ),
      AuxBlockChainBranch("00".bytes, Seq.empty, "00000000".bytes),
      ParentBlockHeader(
        536870912,
        "88bd1abbb9cfeba89271851833730276d66b5d03c2d182000000000000000000".bytes,
        "a7413ab8e4f7e04fdb0b2257a8b89bbc19422d9a8147c5c3a7070d741964d8a5".bytes,
        1506767321, "18ff0018".bytes, 130324938
      )
    )

    expectedNamecoinBlock.withAuxPOW(auxPOW)
  }

  lazy val expectedEnrichedNamecoinBlockWithAuxPOW: EnrichedBitcoinBlockWithAuxPOW = {
    val transactionHashes = Seq(
      "09290e6cec140bb374bc8543a248442711a0b6965e0a9da9cbc3a00cb6621068".bytes,
      "ad1fa5ee9c97ae843691e5800fef87d6270700b2178478bf47025def8284f2aa".bytes,
      "87149cd48023a61a197f251f66219909868fd4d70d55971d46da876a489ec8d7".bytes,
      "a36a02df62547c54f4a862a6397ffef086ba500b759f93c300e0afa3b1c07d29".bytes,
      "c27c38b24c511b5736b724bff63300a3abf18914d729c97e7e21c11c2592378c".bytes,
      "da36c859561e70236493989653e7e66dd7f1b391cfca04c5c178f9c6cd915dc4".bytes,
      "b7023d41eede852a869fbd0deaf094b4dfa036583e23b9b3df1b28afb36fbc1b".bytes
    )

    val enrichedTransactions = expectedNamecoinBlock.transactions
      .zip(transactionHashes)
      .map { case (transaction, hash) => transaction.enriched(hash) }

    expectedNamecoinBlock
      .enriched(enrichedTransactions)
      .withAuxPOW(expectedNamecoinBlockWithAuxPOW.auxPOW)
  }

  lazy val expectedScriptWitness: BitcoinBlock = {
    BitcoinBlock(
      999275, "f9beb4d9".bytes, 536870914, 1503889880, "e93c0118".bytes, 184429655, 470L,
      "35bc73fe8a9800b502e9890f657d85166e498a9672a31e000000000000000000".bytes,
      "836b2d3d0347b6d34f945fe927b5d38a53d5625576c1787585f893bb8406692a".bytes,
      Seq.empty
    )
  }

  lazy val expectedScriptWitness2Blocks: Seq[BitcoinBlock] = {
    Seq(
      BitcoinBlock(1000031, "f9beb4d9".bytes, 536870912, 1503863706, "e93c0118".bytes, -706531299, 2191L,
        "bd9fcf5ba15be6bc78e2d8ff2c4f7a95d9774544a5de37000000000000000000".bytes,
        "1296090cef88e667a56b784781c03d904ee77b9688657e45fe101b1e3a8d316d".bytes,
        Seq.empty
      ),
      BitcoinBlock(999304, "f9beb4d9".bytes, 536870912, 1503836377, "e93c0118".bytes, -566627396, 2508L,
        "9dadb0fccad28f83386f56480dfc44b8c572d5446a8955000000000000000000".bytes,
        "95f35d2323d2b8957eed2dd77e3891a4b2c0b98418bf4c769cb9ad63b685aa75".bytes,
        Seq.empty
      )
    )
  }
}
