package es.upm.fi.dia.oeg.morph.base.query

/**
 * OR condition of the where part an atomic abstract query.
 *
 * @param members the member conditions of the OR
 */
class AbstractQueryConditionOr(
        val members: List[AbstractQueryCondition]) extends AbstractQueryCondition(ConditionType.Or) {

    override def hasReference = false

    override def toString: String = {
        "Or(" + members.mkString(", ") + ")"
    }

    override def equals(c: Any): Boolean = {
        c.isInstanceOf[AbstractQueryConditionOr] &&
            this.members == c.asInstanceOf[AbstractQueryConditionOr].members
    }
}
