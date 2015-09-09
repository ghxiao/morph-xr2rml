package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * The No-Operation MongoDB query operator: this class is used when we cannot translate a
 * JSONPath expression into a MongoDB query.
 */
class MongoQueryNodeNop(val jpPath: String) extends MongoQueryNode {

    override def toQueryStringNotFirst = ""
}
