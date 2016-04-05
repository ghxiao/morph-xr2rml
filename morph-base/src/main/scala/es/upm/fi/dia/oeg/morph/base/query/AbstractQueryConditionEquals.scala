package es.upm.fi.dia.oeg.morph.base.query

/**
 * Equality condition of the <i>Where</i> part an atomic abstract query, created during the rewriting
 * process by matching terms of a triple pattern with references from a term map.
 *
 * @param reference the xR2RML reference (e.g. column name or JSONPath expression) on which the condition applies,
 * typically the column name or JSONPath expression
 * @param eqValue object of the equality condition
 */
class AbstractQueryConditionEquals(
        var reference: String,
        val eqValue: Object) extends AbstractQueryCondition(ConditionType.Equals) with IReference {

    override def toString: String = {
        "Equals(" + reference + ", " + eqValue.toString + ")"
    }

    override def equals(a: Any): Boolean = {
        a.isInstanceOf[AbstractQueryConditionEquals] && {
            val c = a.asInstanceOf[AbstractQueryConditionEquals]
            this.reference == c.reference && this.eqValue == c.eqValue
        }
    }

    override def hashCode(): Int = {
        this.toString.hashCode()
    }
}
