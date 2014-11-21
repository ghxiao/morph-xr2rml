package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.Connection

import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument

abstract class MorphBaseDataTranslator(
        val md: MorphBaseMappingDocument,
        val materializer: MorphBaseMaterializer,
        unfolder: MorphBaseUnfolder,
        val dataSourceReader: MorphBaseDataSourceReader,
        
        /** The connection object can be anything: java.sql.Connection for an RDB, MongoDB context etc. */
        connection: Connection,
        
        properties: MorphProperties) {

    val logger = Logger.getLogger(this.getClass().getName());

    def translateData(triplesMap: MorphBaseClassMapping);

    /**
     * @param query may be either an iQuery in the RDB case, or a simple string in case of non row-based nor SQL based databases
     */
    def generateRDFTriples(cm: MorphBaseClassMapping, query: Object);
}