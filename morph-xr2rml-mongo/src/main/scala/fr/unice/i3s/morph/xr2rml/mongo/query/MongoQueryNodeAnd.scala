package fr.unice.i3s.morph.xr2rml.mongo.query

/**
  * MongoDB query starting with an AND node:
 * 	$and: [{doc1}, {doc1}... {docN}]
 */
class MongoQueryNodeAnd(val members: List[MongoQueryNode]) extends MongoQueryNode {

    override def toQueryStringNotFirst() = {
        var str = "$and: ["

        var first = true
        members.foreach(m => {
            if (first)
                first = false
            else
                str += ", "
            str += "{" + m.toQueryString() + "}"
        })
        str += "]"
        str
    }
}