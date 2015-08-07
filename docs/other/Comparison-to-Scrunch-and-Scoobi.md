Scalding comes with an executable tutorial set that does not require a Hadoop
cluster.  If you're curious about Scalding, why not invest a bit of time and run the tutorial
yourself and make your own judgement?

The fact is, the APIs of these systems are all very similar if we compare the TypedAPI of Scalding to Scrunch or Scoobi (or Spark). Scalding is probably the most heavily used in terms of total data processed, given the heavy use at Twitter, Stripe, Etsy and many other places.

# Difference between the Fields API of scalding and Scoobi and Scrunch

Scalding was developed before either of those projects
were announced publicly and has been used in production at Twitter for more than six months
(though it has been through a few iterations internally).
The main difference between Scalding (and Cascading) and Scrunch/Scoobi is that Cascading has
a record model where each element in your distributed list/table is a table with some named
fields.  This is nice because most common cases are to have a few primitive columns (ints, strings,
etc...).  This is discussed in detail in the two answers to the following question:
<http://www.quora.com/Apache-Hadoop/What-are-the-differences-between-Crunch-and-Cascading>

Scoobi and Scrunch stress types and do not
use field names to build ad-hoc record types.  Cascading's fields are very convenient,
and our users have been very productive with Scalding. Fields do present problems for
type inference because Cascading cannot tell you the type of the data in Fields("user_id", "clicks")
at compile time.  This could be surmounted by building a record system in scala that
allows the programmer to express the types of the fields, but the cost of this is not trivial,
and the win is not so clear.

Scalding supports using any scala object in your map/reduce operations using Kryo serialization,
including scala Lists, Sets,
Maps, Tuples, etc.  It is not clear that such transparent serialization is present yet in
scrunch.  Like Scoobi, Scalding has a form of MSCR fusion by relying on Cascading's AggregateBy
operations.  Our Reduce primitives (see GroupBuilder.reduce and .mapReduceMap) are comparable to
Scoobi's combine primitive, which by default uses Hadoop combiners on the map side.

Lastly, Scalding comes with a script that allows you to write a single file and run that
single file locally or on your Hadoop cluster by typing one line: `scald.rb [--local] myJob.scala`.
It is really convenient to use the same language/tool to run jobs on Hadoop and then to post-process
the output locally.