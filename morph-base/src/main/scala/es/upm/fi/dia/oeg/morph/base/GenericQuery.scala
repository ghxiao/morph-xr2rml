package es.upm.fi.dia.oeg.morph.base

import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

class GenericQuery(
        val dbType: Constants.DatabaseType.Value,

        /** The concrete instance of query object: ISqlQuery in case of RDB, MongoDBQuery in case of MongoDB, etc. */
        val concreteQuery: Object,

        /** In the query rewriting context, this is a triples map that is 
         *  bound to the triple pattern from which we have derived this query */
		val boundTriplesMap: Option[R2RMLTriplesMap]) {

    def isSqlQuery: Boolean = {
        dbType == Constants.DatabaseType.Relational
    }

    def isMongoDBQuery: Boolean = {
        dbType == Constants.DatabaseType.MongoDB
    }

    override def toString: String = { concreteQuery.toString }
}