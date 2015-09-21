package es.upm.fi.dia.oeg.morph.base

class GenericQuery(
        val dbType: Constants.DatabaseType.Value,

        /** The concrete instance of query object: ISqlQuery in case of RDB, String in case of MongoDB, etc. */
        val concreteQuery: Object) {

    def isSqlQuery: Boolean = {
        dbType == Constants.DatabaseType.Relational
    }

    def isMongoDBQuery: Boolean = {
        dbType == Constants.DatabaseType.MongoDB
    }
}