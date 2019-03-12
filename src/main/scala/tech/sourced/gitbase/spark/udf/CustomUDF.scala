package tech.sourced.gitbase.spark.udf

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Custom named user defined function.
  */
abstract class CustomUDF {
  /** Name of the function. */
  def name: String

  /** Function to execute when this function is called. */
  def function: UserDefinedFunction

  def apply(exprs: Column*): Column = function.withName(name)(exprs: _*)
}

/**
  * Custom named user defined expression.
  */
abstract class CustomExprFunction {
  /** Name of the expression. */
  def name: String

  /** Function of the expression. */
  def function: FunctionBuilder

}
