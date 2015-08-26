package es.upm.fi.dia.oeg.morph.r2rml.engine

import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

class R2RMLParser {
    def parse(mappingResource: Object): R2RMLMappingDocument = {
        val mappingDocumentPath = mappingResource.asInstanceOf[String];
        val md = R2RMLMappingDocument(mappingDocumentPath);
        return md;
    }
}

