package ro.planet.documentum.stada.modules.word;

import java.util.Iterator;
import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;

public class WordNamespaceContext implements NamespaceContext {

  public String getNamespaceURI(String prefix) {
    if (prefix.equals("w"))
      return "http://schemas.openxmlformats.org/wordprocessingml/2006/main";
    else
      return XMLConstants.NULL_NS_URI;
  }

  public String getPrefix(String namespace) {
    return null;
  }

  public Iterator getPrefixes(String namespace) {
    return null;
  }
}