package benchmark.utils;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SnappyCompress {
    public static void main(String[] args) throws Exception {
        final URL resource = Resources.getResource("large2.json");

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final FramedSnappyCompressorOutputStream snappyOutputStream = new FramedSnappyCompressorOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, snappyOutputStream);
        }

        Files.write(Paths.get("large2.json.snappy"), byteArrayOutputStream.toByteArray());
    }
}
