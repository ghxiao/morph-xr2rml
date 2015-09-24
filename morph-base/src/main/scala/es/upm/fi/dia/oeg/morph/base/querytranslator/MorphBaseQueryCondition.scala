package es.upm.fi.dia.oeg.morph.base.querytranslator

import es.upm.fi.dia.oeg.morph.base.exception.MorphException

/**
 * <p>Abstract representation of a condition created during the rewriting process by matching terms of a triple pattern
 * with references from a term map. This class represents 2 types: Equality and Not-null.</p>
 *
 * Attribute <em>reference<em> is the reference (e.g. column name or JSONPath expression) on which the condition applies,
 * attribute <em>eqValue<em> is used in case of an Equality condition.
 */
class MorphBaseQueryCondition(
        /**
         * The reference on which this condition applies, typically the column name or JSONPath expression
         * from a xrr:reference or rr:template property
         */
        val reference: String,

        /** Query on which the reference applies: child or parent. Necessarily Child in case of a Join condition. */
        val targetQuery: TargetQuery.Value,

        /** The type of condition */
        val condType: ConditionType.Value,

        /** The value in case of an equality condition */
        val eqValue: Object) extends IQueryCondition {

    override def toString: String = {
        condType match {
            case ConditionType.IsNotNull => "NotNull(" + targetQuery + "/" + reference + ")"
            case ConditionType.Equals => "Equals(" + targetQuery + "/" + reference + ", " + eqValue.toString + ")"
            case ConditionType.Join => throw new MorphException("Join condition not supported by class MorphBaseQueryCondition")
        }
    }

    override def equals(a: Any): Boolean = {
        if (a.isInstanceOf[MorphBaseQueryCondition]) {
            val m = a.asInstanceOf[MorphBaseQueryCondition]
            this.reference == m.reference && this.condType == m.condType && this.eqValue == m.eqValue
        }
        else false
    }
}

object MorphBaseQueryCondition {

    /** Constructor for a Not-Null condition */
    def notNull(targetQuery: TargetQuery.Value, ref: String) = {
        new MorphBaseQueryCondition(ref, targetQuery, ConditionType.IsNotNull, null)
    }

    /** Constructor for an Equality condition */
    def equality(targetQuery: TargetQuery.Value, ref: String, value: String) = {
        new MorphBaseQueryCondition(ref, targetQuery, ConditionType.Equals, value)
    }
}