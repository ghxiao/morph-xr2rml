package es.upm.fi.dia.oeg.morph.base.query

/**
 * Is-Null condition of the <i>Where</i> part an atomic abstract query, created during the
 * abstract query optimization phase by the optional-join elimination.
 *
 * @param reference the xR2RML reference (e.g. column name or JSONPath expression) on which the condition applies
 * 
 * @author Franck Michel, I3S laboratory
 */
class AbstractConditionIsNull(
        var reference: String) extends AbstractCondition(ConditionType.IsNull) with IReference {

    override def toString: String = {
        "IsNull(" + reference + ")"
    }

    override def equals(c: Any): Boolean = {
        c.isInstanceOf[AbstractConditionIsNull] &&
            this.reference == c.asInstanceOf[AbstractConditionIsNull].reference
    }

    override def hashCode(): Int = {
        this.toString.hashCode()
    }
}
