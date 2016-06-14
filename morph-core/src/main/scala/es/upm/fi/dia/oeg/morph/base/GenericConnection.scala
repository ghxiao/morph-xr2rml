package es.upm.fi.dia.oeg.morph.base

/**
 * This very simple class is meant to be an umbrella for all types connection to databases.
 * Its main member is concreteCnx that must be cast to a concrete connection class when needed
 *
 * @param dbType type of target database
 * @param concreteCnx the concrete instance of connection object: java.sql.Connection, com.mongodb.MongoClient, etc.
 * 
 * @author Franck Michel, I3S laboratory
 */
class GenericConnection(
        val dbType: Constants.DatabaseType.Value,
        val concreteCnx: Object) {

    def isRelationalDB: Boolean = {
        dbType == Constants.DatabaseType.Relational
    }

    def isMongoDB: Boolean = {
        dbType == Constants.DatabaseType.MongoDB
    }

    override def toString: String = {
        "GenericConnection[" + dbType + ". " + concreteCnx + "]"
    }
}