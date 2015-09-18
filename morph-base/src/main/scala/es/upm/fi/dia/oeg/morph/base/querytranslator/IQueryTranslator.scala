package es.upm.fi.dia.oeg.morph.base.querytranslator

import java.sql.Connection
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.sql.ISqlQuery
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.GenericQuery

trait IQueryTranslator {
    var genCnx: GenericConnection = null;

    var optimizer: MorphBaseQueryOptimizer = null;

    var properties: MorphProperties = null;

    var databaseType: String = null;

    var mappingDocument: R2RMLMappingDocument = null;

    def translate(query: Query): GenericQuery;

    def getPRSQLGen(): MorphBaseProjectionGenerator

    //def translate(op: Op): IQuery;

    //def translateFromQueryFile(queryFilePath: String): IQuery;

    //def translateFromString(queryString: String): IQuery;

    //def translateUpdate(stg: OpBGP): ZUpdate;

    //def translateInsert(stg: OpBGP): ZInsert;

    //def translateDelete(stg: OpBGP): ZDelete;

    //def setConnection(conn: Connection) = { this.connection = conn }

    //def setOptimizer(optimizer: QueryTranslationOptimizer) = { this.optimizer = optimizer }

    //def setDatabaseType(dbType: String) = { this.databaseType = dbType }
}