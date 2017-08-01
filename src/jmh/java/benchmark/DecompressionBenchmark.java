package benchmark;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import com.ning.compress.gzip.OptimizedGZIPInputStream;
import com.ning.compress.lzf.LZFInputStream;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4Factory;
import org.anarres.parallelgzip.ParallelGZIPInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorInputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.zip.GZIPInputStream;

@State(Scope.Benchmark)
public class DecompressionBenchmark {
    @Benchmark
    public void decompressLargeJson_GZIPInputStream_DefaultBufferSize(Blackhole bh) throws IOException {
        final URL resource = Resources.getResource("large2.json.gz");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream)) {
            ByteStreams.copy(gzipInputStream, byteArrayOutputStream);
        }

        bh.consume(byteArrayOutputStream);
    }

    @Benchmark
    public void decompressLargeJson_GZIPInputStream_BufferSize8192(Blackhole bh) throws IOException {
        final URL resource = Resources.getResource("large2.json.gz");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream, 8192)) {
            ByteStreams.copy(gzipInputStream, byteArrayOutputStream);
        }

        bh.consume(byteArrayOutputStream);
    }

    @Benchmark
    public void decompressLargeJson_GZIPInputStream_BufferedInputStream(Blackhole bh) throws IOException {
        final URL resource = Resources.getResource("large2.json.gz");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
             final GZIPInputStream gzipInputStream = new GZIPInputStream(bufferedInputStream)) {
            ByteStreams.copy(gzipInputStream, byteArrayOutputStream);
        }

        bh.consume(byteArrayOutputStream);
    }

    @Benchmark
    public void decompressLargeJson_ParallelGZIPInputStream(Blackhole bh) throws IOException {
        final URL resource = Resources.getResource("large2.json.gz");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final ParallelGZIPInputStream gzipInputStream = new ParallelGZIPInputStream(inputStream)) {
            ByteStreams.copy(gzipInputStream, byteArrayOutputStream);
        }

        bh.consume(byteArrayOutputStream);
    }

    @Benchmark
    public void decompressLargeJson_OptimizedGZIPInputStream(Blackhole bh) throws IOException {
        final URL resource = Resources.getResource("large2.json.gz");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final OptimizedGZIPInputStream gzipInputStream = new OptimizedGZIPInputStream(inputStream)) {
            ByteStreams.copy(gzipInputStream, byteArrayOutputStream);
        }

        bh.consume(byteArrayOutputStream);
    }

    @Benchmark
    public void decompressLargeJson_LZFInputStream(Blackhole bh) throws IOException {
        final URL resource = Resources.getResource("large2.json.lzf");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final LZFInputStream lzfInputStream = new LZFInputStream(inputStream)) {
            ByteStreams.copy(lzfInputStream, byteArrayOutputStream);
        }

        bh.consume(byteArrayOutputStream);
    }

    @Benchmark
    public void decompressLargeJson_GzipCompressorInputStream(Blackhole bh) throws IOException {
        final URL resource = Resources.getResource("large2.json.gz");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final GzipCompressorInputStream gzipInputStream = new GzipCompressorInputStream(inputStream)) {
            ByteStreams.copy(gzipInputStream, byteArrayOutputStream);
        }

        bh.consume(byteArrayOutputStream);
    }

    @Benchmark
    public void decompressLargeJson_LZMACompressorInputStream(Blackhole bh) throws IOException {
        final URL resource = Resources.getResource("large2.json.lzma");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final LZMACompressorInputStream lzmaInputStream = new LZMACompressorInputStream(inputStream)) {
            ByteStreams.copy(lzmaInputStream, byteArrayOutputStream);
        }

        bh.consume(byteArrayOutputStream);
    }

    @Benchmark
    public void decompressLargeJson_SnappyCompressorInputStream(Blackhole bh) throws Exception {
        final URL resource = Resources.getResource("large2.json.snappy");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final FramedSnappyCompressorInputStream snappyInputStream = new FramedSnappyCompressorInputStream(inputStream)) {
            ByteStreams.copy(snappyInputStream, byteArrayOutputStream);
        }

        bh.consume(byteArrayOutputStream);
    }

    @Benchmark
    public void decompressLargeJson_XZCompressorInputStream(Blackhole bh) throws IOException {
        final URL resource = Resources.getResource("large2.json.xz");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final XZCompressorInputStream xzInputStream = new XZCompressorInputStream(inputStream)) {
            ByteStreams.copy(xzInputStream, byteArrayOutputStream);
        }

        bh.consume(byteArrayOutputStream);
    }

    @Benchmark
    public void decompressLargeJson_LZ4BlockInputStream(Blackhole bh) throws IOException {
        final URL resource = Resources.getResource("large2.json.lz4");
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final LZ4BlockInputStream xzInputStream = new LZ4BlockInputStream(inputStream)) {
            ByteStreams.copy(xzInputStream, byteArrayOutputStream);
        }

        bh.consume(byteArrayOutputStream);
    }
}
