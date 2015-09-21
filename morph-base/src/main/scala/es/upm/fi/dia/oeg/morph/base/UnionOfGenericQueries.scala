package es.upm.fi.dia.oeg.morph.base
import scala.collection

/**
 * This class represents a union of concrete queries.
 * In the case of RDBs, it should not be necessary as SQL supports UNIONs.
 * Conversely, MongoDB does not support UNIONs thus a list of queries is returned, each
 * shall be executed independently by the xR2RML processing engine. 
 */
class UnionOfGenericQueries(
        val dbType: Constants.DatabaseType.Value,
        var members: List[GenericQuery]) {

    def this(dbType: Constants.DatabaseType.Value, member: GenericQuery) = this(dbType, List(member))

    def isSqlQuery: Boolean = {
        dbType == Constants.DatabaseType.Relational
    }

    def isMongoDBQuery: Boolean = {
        dbType == Constants.DatabaseType.MongoDB
    }

    def head: GenericQuery = members.head
}