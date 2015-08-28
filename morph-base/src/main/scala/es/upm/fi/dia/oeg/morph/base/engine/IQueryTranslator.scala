package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.Connection
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.algebra.Op
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.TermMapResult
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBasePRSQLGenerator

trait IQueryTranslator {
    var connection: Connection = null;

    var optimizer: QueryTranslationOptimizer = null;

    var properties: MorphProperties = null;

    var databaseType: String = null;

    var mappingDocument: R2RMLMappingDocument = null;

    def translate(query: Query): IQuery;

    def getPRSQLGen(): MorphBasePRSQLGenerator

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