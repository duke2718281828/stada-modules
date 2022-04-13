package ro.planet.documentum.stada.modules.pdf;

import org.docx4j.TraversalUtil;
import org.docx4j.TraversalUtil.CallbackImpl;
import org.docx4j.XmlUtils;
import org.docx4j.openpackaging.packages.WordprocessingMLPackage;
import org.docx4j.wml.CTSimpleField;
import org.docx4j.wml.FldChar;

import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.stream.StreamSource;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class RemoveFieldsHelper extends DocxHelper {

	protected String getProjectFolder() {
		String path = getClass().getResource(".").getPath();
		String mark = "stada-modules";
		int index = path.lastIndexOf(mark);
		path = path.substring(index + mark.length() + 1);
		return "";
	}

	public WordprocessingMLPackage update1(WordprocessingMLPackage wordMLPackage) throws Exception {

		InputStream xsltStream = null;
		try {
			xsltStream = RemoveFieldsHelper.class.getClassLoader().getResourceAsStream("xslt/RemoveFields.xslt");
			Source xsltSource = new StreamSource(xsltStream);
			Templates xslt = XmlUtils.getTransformerTemplate(xsltSource);

			wordMLPackage.transform(xslt, null);
			return wordMLPackage;
		} finally {
			if (xsltStream != null) {
				try {
					xsltStream.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
		}
	}

	public void update(WordprocessingMLPackage wordMLPackage) throws Exception {
		new TraversalUtil(wordMLPackage.getMainDocumentPart(), new CallbackImpl() {

			@Override
			public List<Object> apply(Object obj) {
				debug("{0}", obj.getClass().getName());
				if (obj instanceof CTSimpleField) {
					CTSimpleField fld = (CTSimpleField) obj;
					debug("Simple: {0}", fld.getInstr());
					debug("{0}", marshaltoDebugString(fld));
				} else if (obj instanceof FldChar) {
					FldChar fld = (FldChar) obj;
					debug("Complex: {0}", fld.getFldData());
					//debug("{0}", marshaltoDebugString(((R) fld.getParent()).getParent()));
				}

				return null;

			}
		});

		update1(wordMLPackage);
	}

	private void test() throws Exception {

		String contentStr1 = getProjectFolder() + "resources/TempMergeFieldDoc.Example.docx";

		WordprocessingMLPackage wordMLPackage = WordprocessingMLPackage.load(new File(contentStr1));
		debug("Loaded {0}", contentStr1);

		update1(wordMLPackage);

		wordMLPackage.save(new File("E:/1.docx"));

		try {
			Desktop.getDesktop().open(new File("E:/1.docx"));
		} catch (Throwable tr) {
			throw new RuntimeException(tr);
		}
		// this.traversalSearch(template.getMainDocumentPart(),)

	}

	public static void main(String[] args) {

		try {
			new RemoveFieldsHelper().test();
		} catch (Throwable tr) {
			System.out.println(tr.getClass().getName());
			System.out.println(tr.getMessage());
			tr.printStackTrace();
		}
	}
}
