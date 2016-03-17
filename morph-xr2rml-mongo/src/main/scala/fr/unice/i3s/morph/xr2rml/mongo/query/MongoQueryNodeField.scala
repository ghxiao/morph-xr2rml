package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query element representing a MongoDB path, i.e. a concatenation of field names or array indexes,
 * and a condition set on conditions on that path. Example: "'p': {$gt: 13, $ne: null}"
 *
 * The MongoDB path (field) is the result of translating a JSONPath containing a sequence of field names
 * (with tailing-dot or in array notation) and array indexes, i.e. one of: .p, ["p"] or [i].
 * E.g. '.p.q.r', '.p[10]["r"]'
 *
 * Element "members" is a set of MongoQueryNodes that represent the conditions on that path: MongoQueryNodeCond,
 * MongoQueryNodeElemMatch or MongoQueryNodeCompare.
 * But "members" must not contain another MongoQueryNodeField: "{'p': {'q': {$eq: 1}}}" is invalid in MongoDB.
 *
 * If field is "p" and the next node is an equality condition node, the toString() will result in: "'p': {$eq: 'value'}"
 */
class MongoQueryNodeField(jsPath: String, val members: List[MongoQueryNode], arraySlice: List[MongoQueryProjection]) extends MongoQueryNode {

    val path = MongoQueryNode.dotNotation(jsPath)

    this.projection = arraySlice

    /** Constructor with one element and without array slice projection */
    def this(field: String, members: MongoQueryNode) = { this(field, List(members), List.empty) }

    /** Constructor with a list of elements and without array slice projection */
    def this(field: String, members: List[MongoQueryNode]) = { this(field, members, List.empty) }

    override def isField: Boolean = true

    override def equals(q: Any): Boolean = {
        if (q.isInstanceOf[MongoQueryNodeField]) {
            val qf = q.asInstanceOf[MongoQueryNodeField]
            this.path == qf.path && this.members == qf.members
        } else false
    }

    override def toString() = { "'" + path + "': {" + members.mkString(", ") + "}" }
}
