package org.zuinnote.spark.ethereum.model

final case class EthereumBlockHeader(parentHash: Array[Byte], uncleHash: Array[Byte], coinBase: Array[Byte],
                                     stateRoot: Array[Byte], txTrieRoot: Array[Byte], receiptTrieRoot: Array[Byte],
                                     logsBloom: Array[Byte], difficulty: Array[Byte], timestamp: Long, number: Long,
                                     gasLimit: Long, gasUsed: Long, mixHash: Array[Byte], extraData: Array[Byte],
                                     nonce: Array[Byte])

final case class EthereumTransaction(nonce: Array[Byte], value: Long, receiveAddress: Array[Byte], gasPrice: Long,
                                     gasLimit: Long, data: Array[Byte], sig_v: Array[Byte], sig_r: Array[Byte],
                                     sig_s: Array[Byte])

final case class EthereumBlock(ethereumBlockHeader: EthereumBlockHeader,
                               ethereumTransactions: Seq[EthereumTransaction],
                               uncleHeaders: Seq[EthereumBlockHeader])

final case class EnrichedEthereumTransaction(nonce: Array[Byte], value: Long, receiveAddress: Array[Byte],
                                             gasPrice: Long, gasLimit: Long, data: Array[Byte], sig_v: Array[Byte],
                                             sig_r: Array[Byte], sig_s: Array[Byte], sendAddress: Array[Byte],
                                             hash: Array[Byte])

final case class EnrichedEthereumBlock(ethereumBlockHeader: EthereumBlockHeader,
                                       ethereumTransactions: Seq[EnrichedEthereumTransaction],
                                       uncleHeaders: Seq[EthereumBlockHeader])
