package es.upm.fi.dia.oeg.morph.base.query

/**
 * AND condition of the <i>Where</i> part an atomic abstract query created during the
 * abstract query optimization phase.
 *
 * @param members the member conditions of the AND
 */
class AbstractQueryConditionAnd(
        val members: List[AbstractQueryCondition]) extends AbstractQueryCondition(ConditionType.And) {

    override def hasReference = false

    override def toString: String = {
        "And(" + members.mkString(", ") + ")"
    }

    override def equals(c: Any): Boolean = {
        c.isInstanceOf[AbstractQueryConditionAnd] &&
            this.members == c.asInstanceOf[AbstractQueryConditionAnd].members
    }
}
