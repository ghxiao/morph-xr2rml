package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query starting with an elemMath node:
 * 	$elemMatch: {...}
 */
class MongoQueryNodeElemMatch(val next: MongoQueryNode) extends MongoQueryNode {

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeElemMatch] && this.next == q.asInstanceOf[MongoQueryNodeElemMatch].next
    }

    override def toQueryStringNotFirst() = { "$elemMatch: {" + next + "}" }

}