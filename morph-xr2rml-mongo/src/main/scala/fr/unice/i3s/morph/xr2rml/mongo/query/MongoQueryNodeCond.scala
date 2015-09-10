package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query element representing an equality or not null condition on a field.
 * For an equality:
 * 		$eq: value (non string value)
 *   or
 * 		$eq: "value" (string value)
 * For a not null condition:
 * 		$exists: true, $ne: null
 */
class MongoQueryNodeCond(val cond: MongoQueryNode.CondType.Value, val value: Object) extends MongoQueryNode {

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeCond] && this.cond == q.asInstanceOf[MongoQueryNodeCond].cond && this.value == q.asInstanceOf[MongoQueryNodeCond].value
    }

    override def toQueryStringNotFirst() = {
        cond match {
            case MongoQueryNode.CondType.IsNotNull => "$exists: true, $ne: null"

            case MongoQueryNode.CondType.Equals =>
                if (value.isInstanceOf[String])
                    "$eq: '" + value + "'"
                else
                    "$eq: " + value
        }
    }
}
