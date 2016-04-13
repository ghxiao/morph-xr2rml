package fr.unice.i3s.morph.xr2rml.mongo.query

/**
 * @author Franck Michel, I3S laboratory
 *
 */
class MongoQueryNodeCondNotExists() extends MongoQueryNodeCond {

    override def equals(q: Any): Boolean = { q.isInstanceOf[MongoQueryNodeCondNotExists] }

    override def toString() = { "$exists: false" }
}
