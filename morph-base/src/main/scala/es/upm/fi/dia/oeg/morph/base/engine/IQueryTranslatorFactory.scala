package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

trait IQueryTranslatorFactory {
    def createQueryTranslator(
        mappingDocument: R2RMLMappingDocument,
        conn: GenericConnection,
        properties: MorphProperties): IQueryTranslator;
}