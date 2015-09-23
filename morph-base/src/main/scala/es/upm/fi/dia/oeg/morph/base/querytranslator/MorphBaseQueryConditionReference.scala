package es.upm.fi.dia.oeg.morph.base.querytranslator

/**
 * Simple class to store a reference, that is a column name in an RDB or a JSONPath in a MongoDB,
 * relatively to a query that may be either the child query, or the parent query in
 * case of a referencing object map
 */
class MorphBaseQueryConditionReference(val targetQuery: SourceQuery.Value, val reference: String) {

    override def toString: String = { targetQuery.toString + "/" + reference }

    override def equals(a: Any): Boolean = {
        val m = a.asInstanceOf[MorphBaseQueryConditionReference]
        this.targetQuery == m.targetQuery && this.reference == m.reference
    }
}

object SourceQuery extends Enumeration {
    val Child, Parent = Value
}

object MorphBaseQueryConditionReference {
    def apply(targetQuery: SourceQuery.Value, reference: String) = new MorphBaseQueryConditionReference(targetQuery, reference)
}