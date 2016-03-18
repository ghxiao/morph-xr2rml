package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query element representing an equality condition on a field:
 *
 * "$eq: value" (non string value) or
 * "$eq: 'value'" (string value)
 */
class MongoQueryNodeCondEquals(val value: Object) extends MongoQueryNodeCond {

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeCondEquals] && this.value == q.asInstanceOf[MongoQueryNodeCondEquals].value
    }

    override def toString() = {
        if (value.isInstanceOf[String])
            "$eq: '" + value + "'"
        else
            "$eq: " + value
    }
}
