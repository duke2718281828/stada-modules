package ro.planet.documentum.stada.modules.word;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.MessageFormat;
import java.util.List;
import java.util.Vector;

import com.documentum.fc.client.DfQuery;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfLogger;

public class ImageBaseHelper {

    public String getImageURL(String baseURL, String ID) {
	String result = baseURL;
	result += MessageFormat.format("imageBarcode?action=generate_jpeg&footer={0}&id={0}", ID);
	return result;
    }

    public byte[] load(String path) throws Exception {
	FileInputStream str = new FileInputStream(path);
	try {
	    int l = str.available();
	    byte[] bytes = new byte[l];
	    str.read(bytes);
	    return bytes;
	} finally {
	    str.close();
	}
    }

    public byte[] downloadFromUrl(String urlText) throws IOException {
	InputStream is = null;
	try {
	    URL url = new URL(urlText);
	    URLConnection urlConn = url.openConnection();// connect
	    ByteArrayOutputStream str = new ByteArrayOutputStream();

	    is = urlConn.getInputStream();

	    byte[] buffer = new byte[1024 * 4];
	    int len;

	    while ((len = is.read(buffer)) > 0) {
		str.write(buffer, 0, len);
	    }

	    return str.toByteArray();
	} finally {

	    if (is != null) {
		is.close();
	    }

	}
    }

    public byte[] downloadBarcode(String urlParam, IDfSession session) throws Exception {
	return downloadFromUrl(getBarcodeURL(session), urlParam);
    }

    public byte[] downloadFromUrl(List<String> urlText, String urlParam) throws Exception {
	if (urlText.size() == 0)
	{
	    return null;
	}
	for (String url : urlText) {
	    debug("Download from {0}", url);
	    try {
		String imageURL = getImageURL(url, urlParam);
		debug("Download from URL {0}", imageURL);
		byte[] result = downloadFromUrl(imageURL);
		if (result.length > 0) {
		    return result;
		}
	    } catch (Throwable tr) {
		DfLogger.error(this, "Download error", null, tr);
	    }
	}
	throw new DfException("Download error");
    }

    public List<String> getBarcodeURL(IDfSession session) throws DfException {
	List<String> urls = performStringQuery(
		"select title from dm_sysobject where FOLDER('/dmsConfig/Configuration') and object_name = 'barcode_url'",
		session);
	return urls;
    }

    public final List<String> performStringQuery(String query, IDfSession session) throws DfException {
	Vector<String> result = new Vector<String>();

	DfQuery dfquery = new DfQuery();
	dfquery.setDQL(query);
	IDfCollection coll = null;
	try {
	    coll = dfquery.execute(session, IDfQuery.DF_QUERY);
	    String Attr = coll.getAttr(0).getName();
	    while (coll.next()) {
		String s = coll.getString(Attr);
		if (s != null)
		    result.add(s);
	    }
	} catch (DfException x) {
	    DfLogger.error(this, "Cannot perform query {0}", new String[] { query }, x);
	    throw x;
	} finally {
	    try {
		if (coll != null)
		    coll.close();
	    } catch (DfException ex) {
		DfLogger.error(this, "Error while release session", null, ex);
	    }
	}

	return result;
    }

    protected void debug(String message, Object... params) {
	String string = MessageFormat.format(message, params);
	System.out.println(string);
	// DfLogger.debug(this, message, params, null);
    }
}
