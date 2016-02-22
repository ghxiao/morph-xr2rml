package es.upm.fi.dia.oeg.morph.base.query

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

/**
 * Simple encapsulation of a target database query
 *
 * @param boundTriplesMap in the query rewriting context, this is a triples map that is bound to the triple pattern
 * from which we have derived this query
 * @param dbType the type of target database
 * @param concreteQuery the concrete instance of query object: ISqlQuery in case of RDB, MongoDBQuery in case of MongoDB, etc.
 */
class GenericQuery(

        val boundTriplesMap: Option[R2RMLTriplesMap],
        val dbType: Constants.DatabaseType.Value,
        val concreteQuery: Object,
        iter: Option[String]) {

    def isSqlQuery: Boolean = {
        dbType == Constants.DatabaseType.Relational
    }

    def isMongoDBQuery: Boolean = {
        dbType == Constants.DatabaseType.MongoDB
    }

    override def toString: String = {
        var str = "GenericQuery[" + concreteQuery.toString
        if (iter.isDefined)
            str = str + ", Iterator: " + iter.get
        if (boundTriplesMap.isDefined)
            str = str + ", BoundTM: " + boundTriplesMap.get
        str + "]"
    }
}