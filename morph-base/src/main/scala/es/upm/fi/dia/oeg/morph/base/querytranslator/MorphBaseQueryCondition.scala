package es.upm.fi.dia.oeg.morph.base.querytranslator

import es.upm.fi.dia.oeg.morph.base.exception.MorphException

/**
 * Equality or not-null condition on a field.
 */
class MorphBaseQueryCondition(
        /**
         * The reference on which this condition applies. Typically the column name or JSONPath expression
         *  from xrr:reference or rr:template
         */
        val childRef: MorphBaseQueryConditionReference,

        /** The type of condition */
        val cond: MorphConditionType.Value,

        /** The value in case of an equality condition */
        val eqValue: Object,

        /** the parent reference in case of a join condition */
        val parentRef: MorphBaseQueryConditionReference) {

    override def toString: String = {
        cond match {
            case MorphConditionType.IsNotNull => "NotNull(" + childRef + ")"
            case MorphConditionType.Equals => "Equals(" + childRef + ", " + eqValue.toString + ")"
            case MorphConditionType.Join => "Join(" + childRef + ", " + parentRef + ")"
        }
    }

    override def equals(a: Any): Boolean = {
        val m = a.asInstanceOf[MorphBaseQueryCondition]
        this.childRef == m.childRef && this.cond == m.cond && this.eqValue == m.eqValue && this.parentRef == m.parentRef
    }
}

object MorphConditionType extends Enumeration {
    val Equals, IsNotNull, Join = Value
}

object MorphBaseQueryCondition {

    /** Constructor for a Not-Null condition on a reference of the child query*/
    def notNull(targetQuery: SourceQuery.Value, ref: String) = {
        new MorphBaseQueryCondition(
            MorphBaseQueryConditionReference(targetQuery, ref),
            MorphConditionType.IsNotNull, null, null)
    }

    /** Constructor for an Equality condition */
    def equality(targetQuery: SourceQuery.Value, ref: String, value: String) = {
        new MorphBaseQueryCondition(
            MorphBaseQueryConditionReference(targetQuery, ref),
            MorphConditionType.Equals, value, null)
    }

    /** Constructor for an Join condition */
    def join(childRef: String, parentRef: String) = {
        new MorphBaseQueryCondition(
            MorphBaseQueryConditionReference(SourceQuery.Child, childRef),
            MorphConditionType.Join, null,
            MorphBaseQueryConditionReference(SourceQuery.Parent, parentRef)
        )
    }
}