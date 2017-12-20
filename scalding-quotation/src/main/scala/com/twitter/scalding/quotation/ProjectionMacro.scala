package com.twitter.scalding.quotation

import scala.reflect.macros.blackbox.Context

trait ProjectionMacro extends TreeOps with Liftables {
  val c: Context
  import c.universe.{ TypeName => _, _ }

  def projections(params: List[Tree]): Tree = {

    def typeName(t: Tree) =
      TypeName(t.symbol.typeSignature.typeSymbol.fullName)

    def accessor(m: TermName) =
      Accessor(m.decodedName.toString)

    def typeReference(tpe: Type) =
      TypeReference(TypeName(tpe.typeSymbol.fullName))

    def isFunction(t: Tree) =
      Option(t.symbol).map {
        _.typeSignature
          .erasure
          .typeSymbol
          .fullName
          .contains("scala.Function")
      }.getOrElse(false)

    def functionBodyProjections(param: Tree, inputs: List[Tree], body: Tree): List[Tree] = {

      val inputSymbols = inputs.map(_.symbol).toSet

      object ProjectionExtractor {
        def unapply(t: Tree): Option[Tree] =
          t match {

            case q"$v.$m(..$params)" => unapply(v)

            case q"$v.$m" if t.symbol.isMethod =>

              if (inputSymbols.contains(v.symbol)) {
                val p =
                  TypeReference(typeName(v))
                    .andThen(accessor(m), typeName(t))
                Some(q"$p")
              } else
                unapply(v).map { n =>
                  q"$n.andThen(${accessor(m)}, ${typeName(t)})"
                }

            case t if inputSymbols.contains(t.symbol) =>
              Some(q"${TypeReference(typeName(t))}")

            case _ => None
          }
      }

      def functionCall(func: Tree, params: List[Tree]): Tree = {
        val paramProjections = params.flatMap(ProjectionExtractor.unapply)
        q"""
          $func match {
            case f: _root_.com.twitter.scalding.quotation.QuotedFunction =>
              f.quoted.projections.basedOn($paramProjections.toSet)
            case _ =>
              _root_.com.twitter.scalding.quotation.Projections(Set(..$paramProjections))
          }
        """
      }

      collect(body) {
        case q"$func.apply[..$t](..$params)" =>
          functionCall(func, params)
        case q"$func(..$params)" if isFunction(func) =>
          functionCall(func, params)
        case t @ ProjectionExtractor(p) =>
          q"_root_.com.twitter.scalding.quotation.Projections(Set($p))"
      }
    }

    def functionInstanceProjections(func: Tree): List[Tree] = {
      val paramProjections =
        func.symbol.typeSignature.typeArgs.dropRight(1)
          .map(typeReference)
      q"""
        $func match {
          case f: _root_.com.twitter.scalding.quotation.QuotedFunction =>
            f.quoted.projections
          case _ =>
            _root_.com.twitter.scalding.quotation.Projections(Set(..$paramProjections))
        }
      """ :: Nil
    }

    def methodProjections(method: Tree): List[Tree] = {
      val paramRefs =
        method.symbol.asMethod.paramLists.flatten
          .map(param => typeReference(param.typeSignature))
      q"${Projections(paramRefs.toSet)}" :: Nil
    }

    val nestedList =
      params.flatMap {
        case param @ q"(..$inputs) => $body" =>
          functionBodyProjections(param, inputs, body)

        case func if isFunction(func) =>
          functionInstanceProjections(func)

        case method if method.symbol != null && method.symbol.isMethod =>
          methodProjections(method)

        case other =>
          Nil
      }

    q"_root_.com.twitter.scalding.quotation.Projections.flatten($nestedList)"
  }
}
