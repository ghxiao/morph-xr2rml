package es.upm.fi.dia.oeg.morph.base.querytranslator

import es.upm.fi.dia.oeg.morph.base.exception.MorphException

/**
 * Abstract representation of a condition created during the rewriting process by matching terms of a triple pattern
 * with references from a term map. Those can be of 3 types: Equality, Not-null or Join condition.<br>
 * Attribute <em>queryReference<em> is the reference (e.g. column name or JSONPath expression) on which the condition applies,
 * attribute <em>eqValue<em> is used in case of an Equality condition, <em>parentRef<em> is the reference relating
 * to the parent query in case of a Join condition.
 */
class MorphBaseQueryConditionJoin(

    /** The child reference of the join condition */
    val childRef: String,

    /** The iterator on the result of the child query */
    val childIter: Option[String],

    /** The parent reference of the join condition */
    val parentRef: String,

    /** The iterator on the result of the parent query */
    val parentIter: Option[String])

        extends IQueryCondition {

    /** The type of condition */
    val condType = ConditionType.Join

    override def toString: String = {
        "Join(child/" + childIter.getOrElse("") + "/" + childRef + ", parent/" + parentIter.getOrElse("") + "/" + parentRef + ")"
    }

    override def equals(a: Any): Boolean = {
        if (a.isInstanceOf[MorphBaseQueryConditionJoin]) {
            val m = a.asInstanceOf[MorphBaseQueryConditionJoin]
            this.childRef == m.childRef && this.parentRef == m.parentRef && this.childIter == m.childIter && this.parentIter == m.parentIter
        } else false
    }
}
