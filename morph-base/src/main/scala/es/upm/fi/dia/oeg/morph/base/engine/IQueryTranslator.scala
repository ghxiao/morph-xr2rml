package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.Connection
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.TermMapResult
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import Zql.ZUpdate
import Zql.ZInsert
import Zql.ZDelete

trait IQueryTranslator {
    var connection: Connection = null;

    var optimizer: QueryTranslationOptimizer = null;

    var properties: MorphProperties = null;

    var databaseType: String = null;

    var mappingDocument: MorphBaseMappingDocument = null;

    def translate(query: Query): IQuery;

    //def translate(op: Op): IQuery;

    //def translateFromQueryFile(queryFilePath: String): IQuery;

    //def translateFromString(queryString: String): IQuery;

    def translateResultSet(varName: String, rs: MorphBaseResultSet): TermMapResult;

    //def setDatabaseType(dbType: String) = { this.databaseType = dbType }

    def trans(op: Op): IQuery;

    //def translateUpdate(stg: OpBGP): ZUpdate;

    //def translateInsert(stg: OpBGP): ZInsert;

    //def translateDelete(stg: OpBGP): ZDelete;

    //def setConnection(conn: Connection) = { this.connection = conn }

    //def setOptimizer(optimizer: QueryTranslationOptimizer) = { this.optimizer = optimizer }
}