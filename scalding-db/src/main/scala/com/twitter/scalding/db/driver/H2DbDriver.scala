package com.twitter.scalding.db.driver

case class H2DbDriver() extends JDBCDriver {
  override val driver = DriverClass("org.h2.Driver")
}

