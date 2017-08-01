package benchmark.utils;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import com.ning.compress.lzf.parallel.PLZFOutputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LZ4Compress {
    public static void main(String[] args) throws IOException {
        final URL resource = Resources.getResource("large2.json");

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final LZ4BlockOutputStream lz4BlockOutputStream = new LZ4BlockOutputStream(byteArrayOutputStream, 1 << 16, LZ4Factory.fastestJavaInstance().highCompressor(17))) {
            ByteStreams.copy(inputStream, lz4BlockOutputStream);
        }

        Files.write(Paths.get("large2.json.lz4"), byteArrayOutputStream.toByteArray());
    }
}
