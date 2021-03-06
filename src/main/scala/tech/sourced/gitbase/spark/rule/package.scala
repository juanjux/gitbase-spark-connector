package tech.sourced.gitbase.spark

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{StructField, StructType}

package object rule {

  def getAll: Seq[Rule[LogicalPlan]] = {
    List(
      AddSource,
      PushdownJoins,
      PushdownTree,
      PushdownAggregations
    )
  }

  /**
    * Creates a schema from a list of attributes.
    *
    * @param attributes list of attributes
    * @return resultant schema
    */
  private[rule] def attributesToSchema(attributes: Seq[AttributeReference]): StructType =
    StructType(
      attributes
        .map((a: Attribute) => StructField(a.name, a.dataType, a.nullable, a.metadata))
        .toArray
    )

  private[rule] def containsGroupBy(node: Node): Boolean = {
    (node transformSingleDown {
      case n: GroupBy => Some(n)
      case _ => None
    }).isDefined
  }

  private[rule] def fixAttributeReferences(plan: LogicalPlan): LogicalPlan = {
    import JoinOptimizer._
    val availableAttrs: Seq[Attribute] = plan.children.flatMap(_.output)

    plan.transformExpressionsUp {
      case a: Attribute =>
        val candidates = availableAttrs.filter(attr => attr.name == a.name)
        if (candidates.nonEmpty) {
          candidates.find(attr => getSource(a) == getSource(attr))
              .getOrElse(candidates.head)
        } else {
          a
        }
      case x => x
    }
  }

}
