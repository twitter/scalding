package com.twitter.scalding.quotation

import language.experimental.macros
import scala.reflect.macros.blackbox.Context
import scala.reflect.internal.util.RangePosition
import scala.reflect.internal.util.OffsetPosition
import scala.reflect.macros.runtime.{ Context => ReflectContext }
import java.io.File

class QuotedMacro(val c: Context)
  extends TreeOps
  with TextMacro
  with ProjectionMacro
  with Liftables {
  import c.universe._

  def internal: Tree = quoted

  def method: Tree = {
    rejectScaldingSources
    quoted
  }

  private def quoted: Tree =
    quoted(
      c.asInstanceOf[ReflectContext]
        .callsiteTyper
        .context
        .tree
        .asInstanceOf[Tree])

  val QuotedCompanion = q"_root_.com.twitter.scalding.quotation.Quoted"

  private def quoted(tree: Tree): Tree = {
    val source = Source(tree.pos.source.path, tree.pos.line)

    find(tree) { t =>
      t.pos != NoPosition && t.pos.start <= c.enclosingPosition.start
    }.flatMap { t =>
      collect(t) {

        // the start position of vals is wrong, so we workaround
        case q"val $name = $body" => quoted(body)

        case q"$m.method" if m.symbol.fullName == classOf[Quoted].getName =>
          c.abort(
            c.enclosingPosition,
            "Quoted.method can be invoked only as an implicit parameter")

        case tree @ q"$instance.$method[..$t]" =>
          q"${Quoted(source, Some(callText(method, t)), Projections.empty)}"

        case tree @ q"$instance.$method[..$t](...$params)" =>
          q"""
            $QuotedCompanion(
              $source, 
              Some(${callText(method, t ++ params.flatten)}), 
              ${projections(params.flatten)})
          """

      }.headOption
    }.getOrElse {
      q"${Quoted(source, None, Projections.empty)}"
    }
  }

  def function(f: Tree): Tree = {
    val source = Source(f.pos.source.path, f.pos.line)
    val text = paramsText(TermName("function"), f)
    f match {
      case q"(..$params) => $body" =>
        c.untypecheck {
          q"""
            new ${f.tpe.finalResultType} with ${c.symbolOf[QuotedFunction]} {
              override def apply(..$params) = $body
              override def quoted = 
                $QuotedCompanion(
                  $source, 
                  Some($text), 
                  ${projections(f :: Nil)}
                )
            }
          """
        }
      case _ =>
        c.abort(f.pos, "Expected a function")
    }
  }

  private def rejectScaldingSources = {

    def whitelist =
      Set("test", "example", "tutorial")
        .exists(c.enclosingPosition.source.path.contains)

    def isScalding(sym: Symbol): Boolean =
      sym.fullName.startsWith("com.twitter.scalding") || {
        sym.owner match {
          case NoSymbol => false
          case owner => isScalding(owner)
        }
      }

    if (!whitelist && isScalding(c.internal.enclosingOwner))
      c.abort(
        c.enclosingPosition,
        "The quotation must happen at the level of the user-facing API. Add an `implicit q: Quoted` to the enclosing method. " +
          "If that's not possible and the transformation doesn't introduce projections, use Quoted.internal.")
  }
}

