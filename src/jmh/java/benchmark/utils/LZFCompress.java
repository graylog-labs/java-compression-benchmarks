package benchmark.utils;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import com.ning.compress.lzf.LZFOutputStream;
import com.ning.compress.lzf.parallel.PLZFOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LZFCompress {
    public static void main(String[] args) throws IOException {
        final URL resource = Resources.getResource("large2.json");

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final PLZFOutputStream lzfOutputStream = new PLZFOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, lzfOutputStream);
        }

        Files.write(Paths.get("large2.json.lzf"), byteArrayOutputStream.toByteArray());
    }
}
