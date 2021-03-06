package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UastExtractParse extends CustomUDF {
  override def name: String = "uast_extract_parse"
  override def function: UserDefinedFunction = udf(JsonArrayParser.extract _)
}
