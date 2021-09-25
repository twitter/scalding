package com.twitter.scalding.typed

import java.util

import reflect.ClassTag

sealed trait HashEqualsArrayWrapper[T] {
  def wrapped: Array[T]
}

object HashEqualsArrayWrapper {

  /**
   * Wraps an Array in an object with a valid equals() and hashCode()
   * Uses specialized wrappers for arrays of primitive values.
   */
  def wrap[T](a: Array[T]): HashEqualsArrayWrapper[T] =
    wrapByClassFn[T](a.getClass.asInstanceOf[Class[Array[T]]])(a)

  /**
   * Creates a function that can be used to wrap Arrays into objects
   * with valid equals() and hashCode() methods.
   *
   * Using this method and applying it to many arrays should be faster
   * than using wrap above on each array, because this method uses reflection
   * once, and wrap above uses reflection on each individual array.
   */
  def wrapByClassFn[T](clazz: Class[Array[T]]): Array[T] => HashEqualsArrayWrapper[T] = {

    val fn = clazz match {
      case c if classOf[Array[Long]].equals(c) => a: Array[Long] => new HashEqualsLongArrayWrapper(a)
      case c if classOf[Array[Int]].equals(c) => a: Array[Int] => new HashEqualsIntArrayWrapper(a)
      case c if classOf[Array[Short]].equals(c) => a: Array[Short] => new HashEqualsShortArrayWrapper(a)
      case c if classOf[Array[Char]].equals(c) => a: Array[Char] => new HashEqualsCharArrayWrapper(a)
      case c if classOf[Array[Byte]].equals(c) => a: Array[Byte] => new HashEqualsByteArrayWrapper(a)
      case c if classOf[Array[Boolean]].equals(c) => a: Array[Boolean] => new HashEqualsBooleanArrayWrapper(a)
      case c if classOf[Array[Float]].equals(c) => a: Array[Float] => new HashEqualsFloatArrayWrapper(a)
      case c if classOf[Array[Double]].equals(c) => a: Array[Double] => new HashEqualsDoubleArrayWrapper(a)
      case c => a: Array[T] => new HashEqualsObjectArrayWrapper(a)
    }

    fn.asInstanceOf[(Array[T] => HashEqualsArrayWrapper[T])]
  }

  /**
   * ct.runtimeClass returns Class[_] so here we cast
   */
  private[typed] def classForTag[T](ct: ClassTag[T]): Class[T] = ct.runtimeClass.asInstanceOf[Class[T]]

  def wrapByClassTagFn[T: ClassTag]: Array[T] => HashEqualsArrayWrapper[T] =
    wrapByClassFn(classForTag(implicitly[ClassTag[T]].wrap))

  implicit val longArrayOrd: Ordering[Array[Long]] = new Ordering[Array[Long]] {
    override def compare(x: Array[Long], y: Array[Long]): Int = {
      val lenCmp = java.lang.Integer.compare(x.length, y.length)

      if (lenCmp != 0) {
        lenCmp
      } else if (x.length == 0) {
        0
      } else {
        val len = x.length
        var i = 1
        var cmp = java.lang.Long.compare(x(0), y(0))
        while (i < len && cmp == 0) {
          cmp = java.lang.Long.compare(x(i), y(i))
          i = i + 1
        }
        cmp
      }
    }
  }

  implicit val intArrayOrd: Ordering[Array[Int]] = new Ordering[Array[Int]] {
    override def compare(x: Array[Int], y: Array[Int]): Int = {
      val lenCmp = java.lang.Integer.compare(x.length, y.length)

      if (lenCmp != 0) {
        lenCmp
      } else if (x.length == 0) {
        0
      } else {
        val len = x.length
        var i = 1
        var cmp = java.lang.Integer.compare(x(0), y(0))
        while (i < len && cmp == 0) {
          cmp = java.lang.Integer.compare(x(i), y(i))
          i = i + 1
        }
        cmp
      }
    }
  }

  implicit val shortArrayOrd: Ordering[Array[Short]] = new Ordering[Array[Short]] {
    override def compare(x: Array[Short], y: Array[Short]): Int = {
      val lenCmp = java.lang.Integer.compare(x.length, y.length)

      if (lenCmp != 0) {
        lenCmp
      } else if (x.length == 0) {
        0
      } else {
        val len = x.length
        var i = 1
        var cmp = java.lang.Short.compare(x(0), y(0))
        while (i < len && cmp == 0) {
          cmp = java.lang.Short.compare(x(i), y(i))
          i = i + 1
        }
        cmp
      }
    }
  }

  implicit val charArrayOrd: Ordering[Array[Char]] = new Ordering[Array[Char]] {
    override def compare(x: Array[Char], y: Array[Char]): Int = {
      val lenCmp = java.lang.Integer.compare(x.length, y.length)

      if (lenCmp != 0) {
        lenCmp
      } else if (x.length == 0) {
        0
      } else {
        val len = x.length
        var i = 1
        var cmp = java.lang.Character.compare(x(0), y(0))
        while (i < len && cmp == 0) {
          cmp = java.lang.Character.compare(x(i), y(i))
          i = i + 1
        }
        cmp
      }
    }
  }

  implicit val byteArrayOrd: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    override def compare(x: Array[Byte], y: Array[Byte]): Int = {
      val lenCmp = java.lang.Integer.compare(x.length, y.length)

      if (lenCmp != 0) {
        lenCmp
      } else if (x.length == 0) {
        0
      } else {
        val len = x.length
        var i = 1
        var cmp = java.lang.Byte.compare(x(0), y(0))
        while (i < len && cmp == 0) {
          cmp = java.lang.Byte.compare(x(i), y(i))
          i = i + 1
        }
        cmp
      }
    }
  }

  implicit val booleanArrayOrd: Ordering[Array[Boolean]] = new Ordering[Array[Boolean]] {
    override def compare(x: Array[Boolean], y: Array[Boolean]): Int = {
      val lenCmp = java.lang.Integer.compare(x.length, y.length)

      if (lenCmp != 0) {
        lenCmp
      } else if (x.length == 0) {
        0
      } else {
        val len = x.length
        var i = 1
        var cmp = java.lang.Boolean.compare(x(0), y(0))
        while (i < len && cmp == 0) {
          cmp = java.lang.Boolean.compare(x(i), y(i))
          i = i + 1
        }
        cmp
      }
    }
  }

  implicit val floatArrayOrd: Ordering[Array[Float]] = new Ordering[Array[Float]] {
    override def compare(x: Array[Float], y: Array[Float]): Int = {
      val lenCmp = java.lang.Integer.compare(x.length, y.length)

      if (lenCmp != 0) {
        lenCmp
      } else if (x.length == 0) {
        0
      } else {
        val len = x.length
        var i = 1
        var cmp = java.lang.Float.compare(x(0), y(0))
        while (i < len && cmp == 0) {
          cmp = java.lang.Float.compare(x(i), y(i))
          i = i + 1
        }
        cmp
      }
    }
  }

  implicit val doubleArrayOrd: Ordering[Array[Double]] = new Ordering[Array[Double]] {
    override def compare(x: Array[Double], y: Array[Double]): Int = {
      val lenCmp = java.lang.Integer.compare(x.length, y.length)

      if (lenCmp != 0) {
        lenCmp
      } else if (x.length == 0) {
        0
      } else {
        val len = x.length
        var i = 1
        var cmp = java.lang.Double.compare(x(0), y(0))
        while (i < len && cmp == 0) {
          cmp = java.lang.Double.compare(x(i), y(i))
          i = i + 1
        }
        cmp
      }
    }
  }

  implicit val hashEqualsLongOrdering: Ordering[HashEqualsArrayWrapper[Long]] = new Ordering[HashEqualsArrayWrapper[Long]] {
    override def compare(x: HashEqualsArrayWrapper[Long], y: HashEqualsArrayWrapper[Long]): Int =
      longArrayOrd.compare(x.wrapped, y.wrapped)
  }

  implicit val hashEqualsIntOrdering: Ordering[HashEqualsArrayWrapper[Int]] = new Ordering[HashEqualsArrayWrapper[Int]] {
    override def compare(x: HashEqualsArrayWrapper[Int], y: HashEqualsArrayWrapper[Int]): Int =
      intArrayOrd.compare(x.wrapped, y.wrapped)
  }

  implicit val hashEqualsShortOrdering: Ordering[HashEqualsArrayWrapper[Short]] = new Ordering[HashEqualsArrayWrapper[Short]] {
    override def compare(x: HashEqualsArrayWrapper[Short], y: HashEqualsArrayWrapper[Short]): Int =
      shortArrayOrd.compare(x.wrapped, y.wrapped)
  }

  implicit val hashEqualsCharOrdering: Ordering[HashEqualsArrayWrapper[Char]] = new Ordering[HashEqualsArrayWrapper[Char]] {
    override def compare(x: HashEqualsArrayWrapper[Char], y: HashEqualsArrayWrapper[Char]): Int =
      charArrayOrd.compare(x.wrapped, y.wrapped)
  }

  implicit val hashEqualsByteOrdering: Ordering[HashEqualsArrayWrapper[Byte]] = new Ordering[HashEqualsArrayWrapper[Byte]] {
    override def compare(x: HashEqualsArrayWrapper[Byte], y: HashEqualsArrayWrapper[Byte]): Int =
      byteArrayOrd.compare(x.wrapped, y.wrapped)
  }

  implicit val hashEqualsBooleanOrdering: Ordering[HashEqualsArrayWrapper[Boolean]] = new Ordering[HashEqualsArrayWrapper[Boolean]] {
    override def compare(x: HashEqualsArrayWrapper[Boolean], y: HashEqualsArrayWrapper[Boolean]): Int =
      booleanArrayOrd.compare(x.wrapped, y.wrapped)
  }

  implicit val hashEqualsFloatOrdering: Ordering[HashEqualsArrayWrapper[Float]] = new Ordering[HashEqualsArrayWrapper[Float]] {
    override def compare(x: HashEqualsArrayWrapper[Float], y: HashEqualsArrayWrapper[Float]): Int =
      floatArrayOrd.compare(x.wrapped, y.wrapped)
  }

  implicit val hashEqualsDoubleOrdering: Ordering[HashEqualsArrayWrapper[Double]] = new Ordering[HashEqualsArrayWrapper[Double]] {
    override def compare(x: HashEqualsArrayWrapper[Double], y: HashEqualsArrayWrapper[Double]): Int =
      doubleArrayOrd.compare(x.wrapped, y.wrapped)
  }

}

final class HashEqualsLongArrayWrapper(override val wrapped: Array[Long]) extends HashEqualsArrayWrapper[Long] {
  override def hashCode(): Int = util.Arrays.hashCode(wrapped)
  override def equals(obj: scala.Any): Boolean = obj match {
    case other: HashEqualsLongArrayWrapper => util.Arrays.equals(wrapped, other.wrapped)
    case _ => false
  }
}

final class HashEqualsIntArrayWrapper(override val wrapped: Array[Int]) extends HashEqualsArrayWrapper[Int] {
  override def hashCode(): Int = util.Arrays.hashCode(wrapped)
  override def equals(obj: scala.Any): Boolean = obj match {
    case other: HashEqualsIntArrayWrapper => util.Arrays.equals(wrapped, other.wrapped)
    case _ => false
  }
}

final class HashEqualsShortArrayWrapper(override val wrapped: Array[Short]) extends HashEqualsArrayWrapper[Short] {
  override def hashCode(): Int = util.Arrays.hashCode(wrapped)
  override def equals(obj: scala.Any): Boolean = obj match {
    case other: HashEqualsShortArrayWrapper => util.Arrays.equals(wrapped, other.wrapped)
    case _ => false
  }
}

final class HashEqualsCharArrayWrapper(override val wrapped: Array[Char]) extends HashEqualsArrayWrapper[Char] {
  override def hashCode(): Int = util.Arrays.hashCode(wrapped)
  override def equals(obj: scala.Any): Boolean = obj match {
    case other: HashEqualsCharArrayWrapper => util.Arrays.equals(wrapped, other.wrapped)
    case _ => false
  }
}

final class HashEqualsByteArrayWrapper(override val wrapped: Array[Byte]) extends HashEqualsArrayWrapper[Byte] {
  override def hashCode(): Int = util.Arrays.hashCode(wrapped)
  override def equals(obj: scala.Any): Boolean = obj match {
    case other: HashEqualsByteArrayWrapper => util.Arrays.equals(wrapped, other.wrapped)
    case _ => false
  }
}

final class HashEqualsBooleanArrayWrapper(override val wrapped: Array[Boolean]) extends HashEqualsArrayWrapper[Boolean] {
  override def hashCode(): Int = util.Arrays.hashCode(wrapped)
  override def equals(obj: scala.Any): Boolean = obj match {
    case other: HashEqualsBooleanArrayWrapper => util.Arrays.equals(wrapped, other.wrapped)
    case _ => false
  }
}

final class HashEqualsFloatArrayWrapper(override val wrapped: Array[Float]) extends HashEqualsArrayWrapper[Float] {
  override def hashCode(): Int = util.Arrays.hashCode(wrapped)
  override def equals(obj: scala.Any): Boolean = obj match {
    case other: HashEqualsFloatArrayWrapper => util.Arrays.equals(wrapped, other.wrapped)
    case _ => false
  }
}

final class HashEqualsDoubleArrayWrapper(override val wrapped: Array[Double]) extends HashEqualsArrayWrapper[Double] {
  override def hashCode(): Int = util.Arrays.hashCode(wrapped)

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: HashEqualsDoubleArrayWrapper => util.Arrays.equals(wrapped, other.wrapped)
    case _ => false
  }
}

final class HashEqualsObjectArrayWrapper[T](override val wrapped: Array[T]) extends HashEqualsArrayWrapper[T] {
  private val wrappedInternal = wrapped.toSeq
  override def hashCode(): Int = wrappedInternal.hashCode()
  override def equals(obj: scala.Any): Boolean = obj match {
    case other: HashEqualsObjectArrayWrapper[T] => wrappedInternal.equals(other.wrappedInternal)
    case _ => false
  }
}
