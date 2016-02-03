package es.upm.fi.dia.oeg.morph.base.querytranslator

import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.AbstractQuery

trait IQueryTranslator {
    var genCnx: GenericConnection = null;

    var optimizer: MorphBaseQueryOptimizer = null;

    var properties: MorphProperties = null;

    var databaseType: String = null;

    var mappingDocument: R2RMLMappingDocument = null;

    def translate(query: Query): AbstractQuery;

}