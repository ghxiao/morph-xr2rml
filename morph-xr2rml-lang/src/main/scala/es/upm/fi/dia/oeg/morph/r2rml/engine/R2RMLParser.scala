package es.upm.fi.dia.oeg.morph.r2rml.engine

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseParser
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

class R2RMLParser extends MorphBaseParser {
    override def parse(mappingResource: Object): MorphBaseMappingDocument = {
        val mappingDocumentPath = mappingResource.asInstanceOf[String];
        val md = R2RMLMappingDocument(mappingDocumentPath);
        return md;
    }
}

