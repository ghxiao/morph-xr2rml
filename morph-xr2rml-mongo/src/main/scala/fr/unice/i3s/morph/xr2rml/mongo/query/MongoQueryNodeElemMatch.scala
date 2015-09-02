package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query starting with an elemMath node:
 * 	$elemMatch: {...}
 */
class MongoQueryNodeElemMatch(val next: MongoQueryNode) extends MongoQueryNode {

    override def toQueryStringNotFirst() = { "$elemMatch: {" + next.toQueryString + "}" }

}