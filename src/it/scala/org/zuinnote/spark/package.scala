package org.zuinnote

package object spark {
  private[spark] implicit class Unhex(val value: String) extends AnyVal {
    def bytes: Seq[Byte] = value.grouped(2).map(Integer.parseInt(_, 16).toByte).toSeq
  }

  private[spark] implicit class Hex(val value: Iterable[Byte]) extends AnyVal {
    def hex: String = value.map("%02x" format _).mkString
  }
}
