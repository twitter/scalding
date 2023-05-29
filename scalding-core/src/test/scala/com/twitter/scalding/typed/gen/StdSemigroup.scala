package com.twitter.scalding.typed.gen

import com.twitter.algebird.Semigroup

object StdSemigroup {
  implicit val byteGroup: Semigroup[Byte] = Semigroup.from { case (l, r) =>
    implicitly[Numeric[Byte]].plus(l, r)
  }

  implicit val charGroup: Semigroup[Char] = Semigroup.from { case (l, r) =>
    implicitly[Numeric[Char]].plus(l, r)
  }

  implicit val stringGroup: Semigroup[String] = Semigroup.from { case (l, r) =>
    l + r
  }
}
