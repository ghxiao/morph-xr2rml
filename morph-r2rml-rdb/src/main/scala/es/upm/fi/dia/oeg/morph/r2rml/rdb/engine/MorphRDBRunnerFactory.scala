package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.QueryTranslationOptimizerFactory
import java.io.OutputStream
import java.io.Writer

class MorphRDBRunnerFactory extends MorphBaseRunnerFactory {

    override def createRunner(
        mappingDocument: MorphBaseMappingDocument,
        unfolder: MorphBaseUnfolder,
        dataTranslator: Option[MorphBaseDataTranslator],
        queryTranslator: Option[IQueryTranslator],
        resultProcessor: Option[AbstractQueryResultTranslator],
        outputStream: Writer): MorphRDBRunner = {
        new MorphRDBRunner(
            mappingDocument.asInstanceOf[R2RMLMappingDocument],
            unfolder.asInstanceOf[MorphRDBUnfolder],
            dataTranslator.asInstanceOf[Option[MorphRDBDataTranslator]],
            queryTranslator, resultProcessor, outputStream)
    }

    override def readMappingDocumentFile(mappingDocumentFile: String, props: MorphProperties, connection: Connection): MorphBaseMappingDocument = {
        val mappingDocument = R2RMLMappingDocument(mappingDocumentFile, props, connection);
        mappingDocument
    }

    override def createUnfolder(md: MorphBaseMappingDocument, props: MorphProperties): MorphRDBUnfolder = {
        val unfolder = new MorphRDBUnfolder(md.asInstanceOf[R2RMLMappingDocument], props);
        unfolder.dbType = props.databaseType;
        unfolder;
    }

    override def createDataTranslator(
        mappingDocument: MorphBaseMappingDocument,
        materializer: MorphBaseMaterializer,
        unfolder: MorphBaseUnfolder,
        dataSourceReader: MorphBaseDataSourceReader,
        connection: Connection,
        properties: MorphProperties): MorphBaseDataTranslator = {
        new MorphRDBDataTranslator(mappingDocument.asInstanceOf[R2RMLMappingDocument], materializer, unfolder.asInstanceOf[MorphRDBUnfolder], dataSourceReader.asInstanceOf[MorphRDBDataSourceReader], connection, properties);
    }
}

object MorphRDBRunnerFactory {

}