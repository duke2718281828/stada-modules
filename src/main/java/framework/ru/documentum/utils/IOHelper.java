package framework.ru.documentum.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * IO Helper.
 */
public class IOHelper {

    public ByteArrayOutputStream toByteArray(InputStream input) throws Exception {
	ByteArrayOutputStream output = new ByteArrayOutputStream();
	copy(input, output);
	return output;
    }

    public int copy(InputStream input, OutputStream output) throws Exception {

	// Do not do this.
	// byte[] bytes = new byte[str.available()];
	// str.read(bytes);
	// out.write(bytes);

	byte[] buffer = new byte[4096 * 64];
	int readed;
	int size = 0;
	while ((readed = input.read(buffer)) != -1) {
	    output.write(buffer, 0, readed);
	    size += readed;
	}

	return size;
    }

    public int copy(File source, File dest) throws Exception {

	FileInputStream in = new FileInputStream(source);

	try {
	    FileOutputStream out = new FileOutputStream(dest);
	    try {
		return copy(in, out);
	    } finally {
		out.close();
	    }
	} finally {
	    in.close();
	}

    }
}
