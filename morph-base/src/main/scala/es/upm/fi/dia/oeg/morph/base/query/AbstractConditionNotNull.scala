package es.upm.fi.dia.oeg.morph.base.query

/**
 * Not-Null condition of the <i>Where</i> part an atomic abstract query, created during the rewriting
 * process by matching terms of a triple pattern with references from a term map.
 *
 * @param reference the xR2RML reference (e.g. column name or JSONPath expression) on which the condition applies
 * 
 * @author Franck Michel, I3S laboratory
 */
class AbstractConditionNotNull(
        var reference: String) extends AbstractCondition(ConditionType.IsNotNull) with IReference {

    override def toString: String = {
        "NotNull(" + reference + ")"
    }

    override def equals(c: Any): Boolean = {
        c.isInstanceOf[AbstractConditionNotNull] &&
            this.reference == c.asInstanceOf[AbstractConditionNotNull].reference
    }

    override def hashCode(): Int = {
        this.toString.hashCode()
    }
}
