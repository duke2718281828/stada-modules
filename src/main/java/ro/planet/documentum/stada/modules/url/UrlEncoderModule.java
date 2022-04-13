package ro.planet.documentum.stada.modules.url;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import com.documentum.fc.client.DfSingleDocbaseModule;
import com.documentum.fc.common.DfLogger;

public class UrlEncoderModule extends DfSingleDocbaseModule {

  public String encode(String urlDecoded) {
    String urlEncoded = null;
    try {
      urlEncoded = URLEncoder.encode(urlDecoded, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      DfLogger.error(this, "Error at encoding url", null, e);
      e.printStackTrace();
    }
    return urlEncoded;
  }

  public String decode(String urlEncoded) {
    String urlDecoded = null;
    try {
      urlDecoded = URLDecoder.decode(urlEncoded, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      DfLogger.error(this, "Error at decoding url", null, e);
      e.printStackTrace();
    }
    return urlDecoded;
  }
}
