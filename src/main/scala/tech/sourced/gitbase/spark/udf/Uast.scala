package tech.sourced.gitbase.spark.udf

import gopkg.in.bblfsh.sdk.v1.protocol.generated.Status
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.bblfsh.client.BblfshClient
import tech.sourced.enry.Enry

case class Uast(exprs: Seq[Expression]) extends Expression with CodegenFallback {

  override def nullable: Boolean = true

  override def toString: String = s"uast(${exprs.mkString(", ")})"

  override def children: Seq[Expression] = exprs

  override def eval(inputRow: InternalRow): Any = {
    val args = exprs.map(_.eval(inputRow))
    var query = ""
    var content: Array[Byte] = null
    var lang = ""

    args.length match {
      case 1 =>
        content = args.head.asInstanceOf[Array[Byte]]
      case 2 =>
        content = args.head.asInstanceOf[Array[Byte]]
        lang = Option(args(1)).map(_.toString).orNull
      case 3 =>
        content = args.head.asInstanceOf[Array[Byte]]
        lang = Option(args(1)).map(_.toString).orNull
        query = Option(args(2)).map(_.toString).orNull
      case ln =>
        throw new SparkException(
          s"invalid arguments provided to uast, expecting 1, 2 or 3, got $ln"
        )
    }

    if (lang == null || content == null || query == null) {
      return null
    }

    if (lang == "") {
      lang = Enry.getLanguage("file", content)
    }

    Uast.get(content, lang, query).orNull
  }

  override def dataType: DataType = BinaryType

}

object Uast extends CustomExprFunction with Logging {

  override def name: String = "uast"
  override def function: FunctionBuilder = exprs => Uast(exprs)

  def apply(cols: Column*): Column = new Column(Uast(cols.map(_.expr)))

  def get(content: Array[Byte],
          lang: String = "",
          query: String = ""): Option[Array[Byte]] =
    try {
      if (content == null || content.isEmpty) {
        return None
      }

      if (!BblfshUtils.isSupportedLanguage(lang)) {
        return None
      }

      val res = BblfshUtils
        .getClient().parse("", new String(content, "UTF-8"), lang)

      if (res.status != Status.OK) {
        log.warn(s"couldn't get UAST : error ${res.status}: ${res.errors.mkString("; ")}")
        return None
      }

      val nodes = query match {
        case "" => Seq(res.uast.get)
        case q: String => BblfshClient.filter(res.uast.get, q)
      }

      BblfshUtils.marshalNodes(nodes)
    } catch {
      case e@(_: RuntimeException | _: Exception) =>
        log.error(s"couldn't get UAST: $e")
        None
    }
}
