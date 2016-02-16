package fr.unice.i3s.morph.xr2rml.mongo.query

class MongoQueryNodeNotExists() extends MongoQueryNode {

    override def equals(q: Any): Boolean = { q.isInstanceOf[MongoQueryNodeNotExists] }

    override def toString() = { "$exists: false" }
}
