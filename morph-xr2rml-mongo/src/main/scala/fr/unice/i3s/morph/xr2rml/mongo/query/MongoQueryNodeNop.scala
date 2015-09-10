package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * The No-Operation MongoDB query operator: this class is used when we cannot translate a
 * JSONPath expression into a MongoDB query.
 */
class MongoQueryNodeNop(val path: String) extends MongoQueryNode {

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeNop] && this.path == q.asInstanceOf[MongoQueryNodeNop].path
    }

    override def toQueryStringNotFirst = ""
}
