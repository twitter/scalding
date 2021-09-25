package com.twitter.scalding.quotation

case class Contact(phone: String)
case class Person(name: String, contact: Contact, alternativeContact: Option[Contact])

object Person {
  val typeReference = TypeReference(typeName[Person])
  val nameProjection = typeReference.andThen(Accessor("name"), typeName[String])
  val contactProjection = typeReference.andThen(Accessor("contact"), typeName[Contact])
  val phoneProjection = contactProjection.andThen(Accessor("phone"), typeName[String])
}