package benchmark.utils;

import com.github.luben.zstd.ZstdOutputStream;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import net.jpountz.lz4.LZ4BlockOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ZstdCompress {
    public static void main(String[] args) throws IOException {
        final URL resource = Resources.getResource("large2.json");

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final ZstdOutputStream zstdOutputStream = new ZstdOutputStream(byteArrayOutputStream, 19)) {
            ByteStreams.copy(inputStream, zstdOutputStream);
        }

        Files.write(Paths.get("large2.json.zst"), byteArrayOutputStream.toByteArray());
    }
}
