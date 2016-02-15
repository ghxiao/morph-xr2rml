package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query of the form:  "p.q": {$exists: true}.
 * Contrary to MongoQueryNodeField, the parameter here may be formed with several field names like "p.0.q".
 */
class MongoQueryNodeExists(val path: String) extends MongoQueryNode {

    val dotNotedPath = MongoQueryNode.dotNotation(path)

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeExists] && this.path == q.asInstanceOf[MongoQueryNodeExists].path
    }

    override def toString() = { "'" + dotNotedPath + "': {$exists: true}" }
}
