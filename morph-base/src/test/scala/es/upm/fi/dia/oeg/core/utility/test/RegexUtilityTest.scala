package es.upm.fi.dia.oeg.core.utility.test

import scala.util.matching.Regex
import scala.collection.mutable.HashMap
import es.upm.fi.dia.oeg.morph.base.TemplateUtility

object RegexUtilityTest extends App {
	
	val templateString0 = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature/{nr}";
	println("templateString0 = " + templateString0);

	val templateColumns = TemplateUtility.getTemplateColumns(templateString0);
	println("templateColumns = " + templateColumns);
	
	val uriString = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature/168";
	println("uriString = " + uriString);
	
	
	val templateValues = TemplateUtility.getTemplateMatching(templateString0, uriString);
	println("templateValues = " + templateValues);
	
}