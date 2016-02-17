package es.upm.fi.dia.oeg.morph.base.querytranslator

import es.upm.fi.dia.oeg.morph.base.exception.MorphException

/**
 * Abstract representation of a condition created during the rewriting process by matching terms of a triple pattern
 * with references from a term map. Those can be of 3 types: Equality, Not-null or SparqlFilter.
 *
 * @param reference<em> the xR2RML reference (e.g. column name or JSONPath expression) on which the condition applies,
 * typically the column name or JSONPath expression
 * @param condType The type of condition
 * @param eqValue value in case of an equality condition. Ignored in another condition type
 */
class MorphBaseQueryCondition(
        val reference: String,
        val condType: ConditionType.Value,
        val eqValue: Object) {

    override def toString: String = {
        condType match {
            case ConditionType.IsNotNull => "NotNull(" + reference + ")"
            case ConditionType.Equals => "Equals(" + reference + ", " + eqValue.toString + ")"
            case ConditionType.SparqlFilter => throw new MorphException("Join condition not supported by class MorphBaseQueryCondition")
        }
    }

    override def equals(a: Any): Boolean = {
        if (a.isInstanceOf[MorphBaseQueryCondition]) {
            val m = a.asInstanceOf[MorphBaseQueryCondition]
            this.reference == m.reference && this.condType == m.condType && this.eqValue == m.eqValue
        } else false
    }
}

object MorphBaseQueryCondition {

    /** Constructor for a Not-Null condition */
    def notNull(ref: String) = {
        new MorphBaseQueryCondition(ref, ConditionType.IsNotNull, null)
    }

    /** Constructor for an Equality condition */
    def equality(ref: String, value: String) = {
        new MorphBaseQueryCondition(ref, ConditionType.Equals, value)
    }
}

object ConditionType extends Enumeration {
    val Equals, IsNotNull, SparqlFilter = Value
}
