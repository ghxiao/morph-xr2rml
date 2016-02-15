package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB query of the form:  "p.q": {$exists: false}.
 * Contrary to MongoQueryNodeField, the parameter here may be formed with several field names like "p.0.q".
 */
class MongoQueryNodeNotExists(val path: String) extends MongoQueryNode {

    val dotNotedPath = MongoQueryNode.dotNotation(path)

    override def equals(q: Any): Boolean = {
        q.isInstanceOf[MongoQueryNodeNotExists] && this.path == q.asInstanceOf[MongoQueryNodeNotExists].path
    }

    override def toString() = { "'" + dotNotedPath + "': {$exists: false}" }
}
