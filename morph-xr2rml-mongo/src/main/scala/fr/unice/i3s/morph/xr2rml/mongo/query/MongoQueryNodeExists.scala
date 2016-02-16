package fr.unice.i3s.morph.xr2rml.mongo.query

class MongoQueryNodeExists() extends MongoQueryNode {

    override def equals(q: Any): Boolean = { q.isInstanceOf[MongoQueryNodeExists] }

    override def toString() = { "$exists: true" }
}
