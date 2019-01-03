package tech.sourced.gitbase.spark.rule

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{
  Alias, And, AttributeReference, Expression, Literal, NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{StructField, StructType}
import tech.sourced.gitbase.spark._

object PushdownTree extends Rule[LogicalPlan] {

  /** @inheritdoc */
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case logical.Project(exp1, logical.Project(exp2, child))
      if containsDuplicates(exp1 ++ exp2) =>
      fixAttributeReferences(logical.Project(exp1 ++ exp2, child))
    case logical.Project(Seq(), child) => child
    case n@logical.Project(
    projectExpressions,
    r@DataSourceV2Relation(_, DefaultReader(servers, _, query))) =>
      if (containsGroupBy(query)) {
        r
      } else if (containsDuplicates(projectExpressions)) {
        fixAttributeReferences(n)
      } else if (!query.hasProjection) {
        // Split expressions in supported and unsupported to push down
        // only the supported ones and keep upside the unsupported
        val (supportedProject, unsupportedProject) =
        splitSupportedAndUnsupportedExpressions(projectExpressions)

        val newSchema = StructType(
          supportedProject.map(e => StructField(e.name, e.dataType, e.nullable, e.metadata))
        )
        val newOutput = supportedProject.map(e =>
          AttributeReference(e.name, e.dataType, e.nullable, e.metadata)(e.exprId, e.qualifier)
        )

        logical.Project(unsupportedProject,
          DataSourceV2Relation(
            newOutput,
            DefaultReader(
              servers,
              newSchema,
              Project(supportedProject, query)
            )
          )
        )
      } else {
        n
      }

    case n@logical.Filter(
    expr,
    DataSourceV2Relation(out, DefaultReader(servers, schema, query))) =>
      if (!canBeHandled(Seq(expr))) {
        fixAttributeReferences(n)
      } else {
        DataSourceV2Relation(
          out,
          DefaultReader(
            servers,
            schema,
            Filter(splitExpressions(expr), query)
          )
        )
      }

    // We should only push down local sorts.
    case n@logical.Sort(
    order,
    false,
    DataSourceV2Relation(out, DefaultReader(servers, schema, query))) =>
      if (!canBeHandled(order.map(_.child))) {
        fixAttributeReferences(n)
      } else {
        DataSourceV2Relation(
          out,
          DefaultReader(
            servers,
            schema,
            Sort(order, query)
          )
        )
      }

    case logical.LocalLimit(
    limit: Literal,
    DataSourceV2Relation(out, DefaultReader(servers, schema, query))) =>
      val limitNumber = limit.value match {
        case n: Int => n.toLong
        case n: Long => n
        case _ => throw new SparkException("limit literal should be a number")
      }

      DataSourceV2Relation(
        out,
        DefaultReader(
          servers,
          schema,
          Limit(limitNumber, query)
        )
      )

    case node: DataSourceV2Relation => node

    case node => fixAttributeReferences(node)
  }

  private def toNamedExpression(expression: Expression): NamedExpression = {
    expression match {
      case e: NamedExpression => e
      case e => Alias(e, e.sql)()
    }
  }

  private def splitExpressions(expression: Expression): Seq[Expression] = {
    expression match {
      case And(left, right) =>
        splitExpressions(left) ++ splitExpressions(right)
      case e => Seq(e)
    }
  }

  private def canBeHandled(exprs: Seq[Expression]): Boolean = {
    exprs.flatMap(x => QueryBuilder.compileExpression(x)).length == exprs.length
  }

  private def containsDuplicates(exprs: Seq[NamedExpression]): Boolean = {
    exprs.groupBy(_.name).values.map(_.length).exists(_ > 1)
  }

  /**
    * Split expressions in supported and unsupported to push down
    * only the supported ones and keep upside the unsupported
    */
  private def splitSupportedAndUnsupportedExpressions(expressions: Seq[Expression]):
  (Seq[NamedExpression], Seq[NamedExpression]) = {
    import scala.collection.mutable
    val unsupported = mutable.Buffer[NamedExpression]()
    val supported = mutable.Buffer[NamedExpression]()

    expressions.foreach(exp => {
      if (canBeHandled(exp :: Nil)) {
        val e = toNamedExpression(exp)
        supported += e
        unsupported += AttributeReference(
          e.name,
          e.dataType,
          e.nullable,
          e.metadata
        )(e.exprId, e.qualifier)
      } else if (!canBeHandled(exp :: Nil) && canBeHandled(exp.children)) {
        unsupported += toNamedExpression(exp.withNewChildren(exp.children.map(c => {
          val child = toNamedExpression(c)
          supported += child
          AttributeReference(
            child.name,
            child.dataType,
            child.nullable,
            child.metadata
          )(child.exprId, child.qualifier)
        })))
      } else if (exp.children.nonEmpty) {
        unsupported += toNamedExpression(exp.withNewChildren(exp.children.map(c => {
          val (s, u) = splitSupportedAndUnsupportedExpressions(c :: Nil)
          if (u.length != 1) {
            throw new SparkException("invalid number of unsupported expressions obtained")
          }

          supported ++= s
          u.head match {
            case e: Alias => e.child // remove aliases from non topmost expressions
            case o => o
          }
        })))
      } else {
        unsupported += toNamedExpression(exp)
      }
    })

    if (unsupported.length != expressions.length) {
      throw new SparkException("unsupported exceptions don't match input expressions")
    }

    (supported.toList, unsupported.toList)
  }

}
