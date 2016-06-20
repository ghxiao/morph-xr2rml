package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryProcessor
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import org.apache.spark.SparkContext

/**
 * @author Franck Michel, I3S laboratory
 *
 */
trait IMorphFactory extends java.io.Serializable {

    def getProperties: MorphProperties

    def getConnection: GenericConnection

    def getMappingDocument: R2RMLMappingDocument

    def getUnfolder: MorphBaseUnfolder

    def getDataSourceReader: MorphBaseDataSourceReader

    def getMaterializer: MorphBaseMaterializer

    def getDataTranslator: MorphBaseDataTranslator

    def getQueryTranslator: MorphBaseQueryTranslator

    def getQueryProcessor: MorphBaseQueryProcessor

    def getSparkContext: SparkContext
} 