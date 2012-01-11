package com.twitter.scalding

import cascading.tuple.Fields

import org.specs._

class FieldImpsTest extends Specification with FieldConversions {
  noDetailedDiffs() //Fixes issue for scala 2.9
  def setAndCheck[T <: Comparable[_]](v : T)(implicit conv : (T) => Fields) {
    val vF = conv(v)
    vF.equals(new Fields(v)) must beTrue
  }
  def setAndCheckS[T <: Comparable[_]](v : Seq[T])(implicit conv : (Seq[T]) => Fields) {
    val vF = conv(v)
    vF.equals(new Fields(v : _*)) must beTrue
  }
  def setAndCheckSym(v : Symbol) {
    val vF : Fields = v
    vF.equals(new Fields(v.toString.tail)) must beTrue
  }
  def setAndCheckSymS(v : Seq[Symbol]) {
    val vF : Fields = v
    vF.equals(new Fields(v.map(_.toString.tail) : _*)) must beTrue
  }
  "Fields conversions" should {
    "convert from ints" in {
      setAndCheck(int2Integer(0))
      setAndCheck(int2Integer(5))
      setAndCheckS(List(1,23,3,4).map(int2Integer))
      setAndCheckS((0 until 10).map(int2Integer))
    }
    "convert from strings" in {
      setAndCheck("hey")
      setAndCheck("world")
      setAndCheckS(List("one","two","three"))
      //Synonym for list
      setAndCheckS(Seq("one","two","three"))
    }
    "convert from symbols" in {
      setAndCheckSym('hey)
      //Shortest length to make sure the tail stuff is working:
      setAndCheckSym('h)
      setAndCheckSymS(List('hey,'world,'symbols))
    }
    "convert from general int tuples" in {
      var vf : Fields = Tuple1(1)
      vf must be_==(new Fields(int2Integer(1)))
      vf = (1,2)
      vf must be_==(new Fields(int2Integer(1),int2Integer(2)))
      vf = (1,2,3)
      vf must be_==(new Fields(int2Integer(1),int2Integer(2),int2Integer(3)))
      vf = (1,2,3,4)
      vf must be_==(new Fields(int2Integer(1),int2Integer(2),int2Integer(3),int2Integer(4)))
    }
    "convert from general string tuples" in {
      var vf : Fields = Tuple1("hey")
      vf must be_==(new Fields("hey"))
      vf = ("hey","world")
      vf must be_==(new Fields("hey","world"))
      vf = ("foo","bar","baz")
      vf must be_==(new Fields("foo","bar","baz"))
    }
    "convert from general symbol tuples" in {
      var vf : Fields = Tuple1('hey)
      vf must be_==(new Fields("hey"))
      vf = ('hey,'world)
      vf must be_==(new Fields("hey","world"))
      vf = ('foo,'bar,'baz)
      vf must be_==(new Fields("foo","bar","baz"))
    }
    "convert to a pair of Fields from a pair of values" in {
      var f2 : (Fields,Fields) = "hey"->"you"
      f2 must be_==((new Fields("hey"),new Fields("you")))

      f2 = 'hey -> 'you
      f2 must be_==((new Fields("hey"),new Fields("you")))

      f2 = (0 until 10) -> 'you
      f2 must be_==((new Fields((0 until 10).map(int2Integer) : _*),new Fields("you")))

      f2 = (('hey, 'world) -> 'other)
      f2 must be_==((new Fields("hey","world"),new Fields("other")))

      f2  = 0 -> 2
      f2 must be_==((new Fields(int2Integer(0)),new Fields(int2Integer(2))))

      f2 = (0, (1,"you"))
      f2 must be_==((new Fields(int2Integer(0)),new Fields(int2Integer(1),"you")))

      f2 = Seq("one","two","three") -> Seq("1","2","3")
      f2 must be_==((new Fields("one","two","three"),new Fields("1","2","3")))
      f2 = List("one","two","three") -> List("1","2","3")
      f2 must be_==((new Fields("one","two","three"),new Fields("1","2","3")))
      f2 = List('one,'two,'three) -> List('n1,'n2,'n3)
      f2 must be_==((new Fields("one","two","three"),new Fields("n1","n2","n3")))
      f2 = List(4,5,6) -> List(1,2,3)
      f2 must be_==((new Fields(int2Integer(4),int2Integer(5),int2Integer(6)),
        new Fields(int2Integer(1),int2Integer(2),int2Integer(3))))
    }
    "correctly see if there are ints" in {
      hasInts(0) must beTrue
      hasInts((0,1)) must beTrue
      hasInts('hey) must beFalse
      hasInts((0,'hey)) must beTrue
      hasInts(('hey,9)) must beTrue
      hasInts(('a,'b)) must beFalse
      def i(xi : Int) = new java.lang.Integer(xi)
      asSet(0) must be_==(Set(i(0)))
      asSet((0,1,2)) must be_==(Set(i(0),i(1),i(2)))
      asSet((0,1,'hey)) must be_==(Set(i(0),i(1),"hey"))
    }
    "correctly determine default modes" in {
      //Default case:
      defaultMode(0,'hey) must be_==(Fields.ALL)
      defaultMode((0,'t),'x) must be_==(Fields.ALL)
      defaultMode(('hey,'x),'y) must be_==(Fields.ALL)
      //Equal:
      defaultMode('hey,'hey) must be_==(Fields.REPLACE)
      defaultMode(('hey,'x),('hey,'x)) must be_==(Fields.REPLACE)
      defaultMode(0,0) must be_==(Fields.REPLACE)
      //Subset/superset:
      defaultMode(('hey,'x),'x) must be_==(Fields.SWAP)
      defaultMode('x, ('hey,'x)) must be_==(Fields.SWAP)
      defaultMode(0, ('hey,0)) must be_==(Fields.SWAP)
      defaultMode(('hey,0),0) must be_==(Fields.SWAP)
    }

  }
}
