# Why "Pack, Unpack" and not "toList"
The field based API toList should not be used if the size of the list in a groupBy is very large/not known in advance. toList doesn't decrease the data size significantly, and it stands a good chance of creating OOM errors if the lists get too long.A good alternative to toList is to use pack/unpack and reduce. Use pack to convert the tuples into an object, then do a groupBy with a reduce function inside it and have your logic to process the grouped items, combine them etc.

Example 1:

```scala
val res_pipe= inputpipe.groupBy('firstname){
 .toList['lastname]
}
```

Example 2:

```scala
case class Person(firstname: String="", lastname: String = "")

val res_pipe = inputpipe.flatMap(('firstname,'lastname)->('firstname,'person)) {
  in: (String, String) =>
    val (firstname,lastname) = in
    val person = Person(firstname= firstname,lastname= lastname)
    (firstname, person)
}
  .groupBy('firstname)
  .reduce('person->'combinedperson) {
    (personAccumulated: Person, person: Person) =>
        val combined_lastname_person = Person(
         firstname = personAccumulated.firstname,
         lastname = personAccumulated.lastname + "," + person.lastname,
         )
         combined_lastname_person
  }.unpack[Person]('combinedperson -> ('firstname,'lastname))
  //comma separated last names
```

