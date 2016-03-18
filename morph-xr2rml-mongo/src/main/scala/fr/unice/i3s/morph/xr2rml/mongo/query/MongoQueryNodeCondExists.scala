package fr.unice.i3s.morph.xr2rml.mongo.query

class MongoQueryNodeCondExists() extends MongoQueryNode {

    override def equals(q: Any): Boolean = { q.isInstanceOf[MongoQueryNodeCondExists] }

    override def toString() = { "$exists: true" }
}
