package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query starting with an elemMath node:
 * 	$elemMatch: {...}
 */
class MongoQueryNodeElemMatch(val members: List[MongoQueryNode]) extends MongoQueryNode {

    def this(member: MongoQueryNode) = this(List(member))

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeElemMatch] && this.members == q.asInstanceOf[MongoQueryNodeElemMatch].members
    }

    override def toQueryStringNotFirst() = {
        var membersStr = ""
        var first = true

        for (mb <- members) {
            if (first) {
                first = false
                membersStr = mb.toString
            } else
                membersStr += ", " + mb.toString

        }

        "$elemMatch: {" + membersStr + "}"
    }
}

