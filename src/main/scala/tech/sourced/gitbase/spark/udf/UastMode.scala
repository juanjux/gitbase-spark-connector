package tech.sourced.gitbase.spark.udf

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BinaryType, DataType}
import tech.sourced.enry.Enry

case class UastMode(exprs: Seq[Expression]) extends Expression with CodegenFallback with Logging {

  override def nullable: Boolean = true

  override def toString: String = s"uast_mode(${exprs.mkString(", ")})"

  override def children: Seq[Expression] = exprs

  private val modes = Seq("annotated", "semantic", "native")

  override def eval(inputRow: InternalRow): Any = {
    val args = exprs.map(_.eval(inputRow))
    var mode = "annotated"
    var content: Array[Byte] = null
    var lang = ""

    args.length match {
      case 2 =>
        mode = Option(args.head).map(_.toString).orNull
        content = args(1).asInstanceOf[Array[Byte]]
      case 3 =>
        mode = Option(args.head).map(_.toString).orNull
        content = args(1).asInstanceOf[Array[Byte]]
        lang = Option(args(2)).map(_.toString).orNull
      case ln =>
        throw new SparkException(
          s"invalid arguments provided to uast_mode, expecting 2 or 3, got $ln"
        )
    }

    if (lang == null || content == null || mode == null) {
      return null
    }

    if (lang == "") {
      lang = Enry.getLanguage("file", content)
    }

    if (!modes.contains(mode)) {
      log.error(s"wrong mode $mode found in call to udf uast_mode")
      null
    } else {
      // TODO: mode is not yet supported in the scala client version used.
      val result = Uast.get(content, lang).orNull
      result
    }
  }

  override def dataType: DataType = BinaryType

}

object UastMode extends CustomExprFunction {
  override def name: String = "uast_mode"

  override def function: FunctionBuilder = exprs => UastMode(exprs)

  def apply(cols: Column*): Column = new Column(UastMode(cols.map(_.expr)))
}
