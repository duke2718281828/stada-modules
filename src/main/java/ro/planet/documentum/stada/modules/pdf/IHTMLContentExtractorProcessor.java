package ro.planet.documentum.stada.modules.pdf;

import java.io.OutputStream;

public interface IHTMLContentExtractorProcessor {

    public OutputStream newFile(String name) throws Exception;
}
