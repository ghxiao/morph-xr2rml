package es.upm.fi.dia.oeg.morph.base.querytranslator

/**
 * Abstract representation of a condition created during the rewriting process by matching terms of a triple pattern
 * with references from a term map. Those can be of 3 types: Equality, Not-null or Join condition.<br>
 */

trait IQueryCondition {
    /** The type of condition */
    val condType: ConditionType.Value
}

object TargetQuery extends Enumeration {
    val Child, Parent = Value
}

object ConditionType extends Enumeration {
    val Equals, IsNotNull, Join = Value
}
