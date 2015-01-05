package com.twitter.scalding_internal.db.macros.upstream.bijection

import scala.language.experimental.macros

trait IsCaseClass[T]

/**
 * This is a tag trait to allow macros to signal, in a uniform way, that a piece of code was generated.
 */
trait MacroGenerated