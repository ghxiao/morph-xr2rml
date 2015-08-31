package fr.unice.i3s.morph.xr2rml.mongo

/**
 * Very simple representation of a Mongo shell query:
 * In query: db.collection.find({ 'a': { $exists: true} })
 * 'collection' is the collection name while "{ 'a': { $exists: true} }" is the query string
 */
class MongoDBQuery(
        val collection: String,
        val query: String) {

    override def toString(): String = {
        "MongoDBQuery[Collection: " + collection + ". Query: " + query + "]"
    }
}