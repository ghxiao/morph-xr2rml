package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query element representing a simple field name, followed by a set of other fields or conditions.   
 * If field is "p" and the next node is an equality condition node, the result will be something like:
 * 		"p": {$eq: "value"}
 * If field is "p" and the next node is another field node "q", then the result will be something like:
 *  	"p.q": ...
 * 
 * Thus, new MongoQueryNodeField("p1, MongoQueryNodeField("p2", new MongoQueryNodeField("p3", ...))) is
 * translated into: "p1.p2.p3": ...
 */
class MongoQueryNodeField(val field: String, val next: MongoQueryNode) extends MongoQueryNode {

    override def isField: Boolean = { true }

    override def toQueryString() = { "'" + this.toQueryStringP() }

    override def toQueryStringNotFirst() = { "." + this.toQueryStringP() }

    private def toQueryStringP(): String = {
        if (next.isField)
            field + next.toQueryStringNotFirst()
        else
            field + "': {" + next.toQueryStringNotFirst() + "}"
    }
}