package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.GenericConnection

trait IQueryTranslatorFactory {
    def createQueryTranslator(
        mappingDocument: MorphBaseMappingDocument,
        conn: GenericConnection,
        properties: MorphProperties): IQueryTranslator;
}