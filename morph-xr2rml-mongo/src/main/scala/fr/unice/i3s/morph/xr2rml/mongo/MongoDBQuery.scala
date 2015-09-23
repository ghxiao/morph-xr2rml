package fr.unice.i3s.morph.xr2rml.mongo

import org.apache.log4j.Logger

/**
 * Very simple representation of a Mongo shell query:<br>
 *
 * In query: db.collection.find({ 'a': { $exists: true} })
 * 'collection' is the collection name while "{ 'a': { $exists: true} }" is the query string
 * <br>
 * Optionally, an iterator of the logical source can be set after object creation.
 */
class MongoDBQuery(
        val collection: String,
        val query: String) {

    var iterator: Option[String] = None

    def setIterator(iter: Option[String]): MongoDBQuery = {
        this.iterator = iter
        this
    }

    override def toString(): String = {
        if (iterator.isDefined)
            "MongoDBQuery[Collection: " + collection + ". Query: " + query + ". Iterator: " + iterator.get + "]"
        else
            "MongoDBQuery[Collection: " + collection + ". Query: " + query + "]"
    }
    
    override def equals(a: Any): Boolean = {
        val m = a.asInstanceOf[MongoDBQuery]
        this.collection == m.collection && this.query == m.query && this.iterator == m.iterator 
    }
}

object MongoDBQuery {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Create a MongoDBQuery with no iterator.
     * <br>
     * A query string looks like this: db.myCollection.find({ 'a': { $exists: true} })
     * This method return a MongoDBQuery instance where collection = myCollection
     * and query string = "{ 'a': { $exists: true} }"
     */
    def parseQueryString(query: String): MongoDBQuery = {

        var tokens = query.trim.split("\\.")
        if (!tokens(0).equals("db")) {
            logger.error("Invalid query string: " + query)
            return null
        }
        val collection = tokens(1)

        // The query string starts after the '(' and finished before the trailing ')'
        tokens = query.split("\\(")
        val queryStr = tokens(1).substring(0, tokens(1).length - 1)

        new MongoDBQuery(collection, queryStr)
    }

    /**
     * Create a MongoDBQuery and initialize the iterator.
     */
    def parseQueryString(query: String, iterator: Option[String]): MongoDBQuery = {
        parseQueryString(query).setIterator(iterator)
    }
}