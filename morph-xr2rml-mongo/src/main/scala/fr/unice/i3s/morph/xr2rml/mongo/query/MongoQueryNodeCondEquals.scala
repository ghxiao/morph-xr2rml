package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query element representing an equality condition on a field:
 *
 * <code>\$eq: value</code> (non string value) or
 * <code>\$eq: 'value'</code> (string value)
 * 
 * @author Franck Michel, I3S laboratory
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

    /**
     * Specific case of the MongoDB _id field that is an ObjectId
     */
    def toStringId() = {
        if (value.isInstanceOf[String])
            "$oid: '" + value + "'"
        else
            "$oid: " + value
    }
}
