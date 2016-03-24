package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query element representing a not null condition on a field:
 * <code>\$exists: true, \$ne: null<code>
 */
class MongoQueryNodeCondIsNull() extends MongoQueryNodeCond {

    override def equals(q: Any): Boolean = { q.isInstanceOf[MongoQueryNodeCondIsNull] }

    override def toString() = { "$eq: null" }
}
