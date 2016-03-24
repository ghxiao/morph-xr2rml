package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * MongoDB projection of the slice of an array, like: <code>'p': {\$slice: -10}</code>
 */
class MongoQueryProjectionArraySlice(val path: String, val index: String) extends MongoQueryProjection {

    val dotNotedPath = MongoQueryNode.dotNotation(path)

    override def equals(q: Any): Boolean = {
        if (q.isInstanceOf[MongoQueryProjectionArraySlice]) {
            val qc = q.asInstanceOf[MongoQueryProjectionArraySlice]
            this.path == qc.path && this.index == qc.index
        } else false
    }

    override def toString() = {
        "'" + dotNotedPath + "': {$slice: " + index + "}"
    }
}
