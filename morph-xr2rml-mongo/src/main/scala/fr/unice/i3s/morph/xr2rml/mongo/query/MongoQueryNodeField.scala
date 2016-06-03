package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query element representing a MongoDB path, i.e. a concatenation of field names or array indexes,
 * and a condition set on conditions on that path.
 *
 * Example: If jsPath is "p" and members contains an equality condition node, the toString() will result in: <code>'p': {\$eq: 'value'}</code>.
 *
 * @param jsPath is the MongoDB path, it results from the translation of a JSONPath containing a sequence of field names
 * (with tailing-dot or in array notation) and array indexes, i.e. one of: .p, ["p"] or [i].
 * E.g. '.p.q.r', '.p[10]["r"]'
 *
 * @param members is a set of MongoQueryNodes that represent the conditions on that path: MongoQueryNodeCond,
 * MongoQueryNodeElemMatch or MongoQueryNodeCompare.
 * But "members" must NOT contain another MongoQueryNodeField since the following is invalid in MongoDB:
 * <code>{'p': {'q': {\$eq: 1}}}</code>
 *
 * @author Franck Michel, I3S laboratory
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

    override def toString(): String = {
        if (path == "_id" && members.size == 1 && members(0).isInstanceOf[MongoQueryNodeCondEquals]) {
            val equals = members(0).asInstanceOf[MongoQueryNodeCondEquals]
            "'_id': {" + equals.toStringId() + "}"
        } else
            "'" + path + "': {" + members.mkString(", ") + "}"
    }
}
