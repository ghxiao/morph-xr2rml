package es.upm.fi.dia.oeg.morph.base

class RDFTermLiteral(
        val v: Object,
        val datatype: Option[String],
        val language: Option[String]) extends RDFTerm(v) {

    override val isLiteral = true

    override def equals(a: Any): Boolean = {
        val term = a.asInstanceOf[RDFTermLiteral]
        this.getClass == a.getClass && value == term.value && datatype == term.datatype && language == term.language
    }

    override def hashCode(): Int = {
        this.getClass.hashCode + value.hashCode() + datatype.hashCode() + language.hashCode()
    }
}