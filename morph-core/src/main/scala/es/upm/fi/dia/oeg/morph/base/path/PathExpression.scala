package es.upm.fi.dia.oeg.morph.base.path

/**
 * @author Franck Michel, I3S laboratory
 */ 
abstract class PathExpression(
        /** Reference value as provided by the mapping file */
        val pathExpression: String) {

    /**
     * Evaluate a value against the path expression and return a list of values
     *
     * @param value the value to parse (XML, JSON, CSV etc.)
     * @return list of values resulting from the evaluation
     */
    def evaluate(value: String): List[Object]
}