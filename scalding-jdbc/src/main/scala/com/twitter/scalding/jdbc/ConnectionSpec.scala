package com.twitter.scalding.jdbc

case class ConnectUrl(get: String)
case class UserName(get: String)
case class Password(get: String)

/**
 * Pass your DB credentials to this class in a preferred secure way
 */
case class ConnectionSpec(connectUrl: ConnectUrl, userName: UserName, password: Password)
