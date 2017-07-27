package benchmark;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import com.ning.compress.gzip.OptimizedGZIPOutputStream;
import com.ning.compress.lzf.LZFOutputStream;
import com.ning.compress.lzf.parallel.PLZFOutputStream;
import org.anarres.parallelgzip.ParallelGZIPOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.apache.commons.compress.compressors.lzma.LZMACompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.tukaani.xz.LZMA2Options;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

@State(Scope.Benchmark)
public class CompressionBenchmark {
    private final boolean printSize = false;
    private final URL resource = Resources.getResource("large2.json");

    @Benchmark
    public int compressLargeJson_GZIPOutputStream_DefaultBufferSize() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_GZIPOutputStream_BufferSize8192() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream, 8192)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_GZIPOutputStream_BufferedOutputStream() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
             final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(gzipOutputStream)) {
            ByteStreams.copy(inputStream, bufferedOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_ParallelGZIPOutputStream() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final ParallelGZIPOutputStream gzipOutputStream = new ParallelGZIPOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_OptimizedGZIPOutputStream() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final OptimizedGZIPOutputStream gzipOutputStream = new OptimizedGZIPOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_LZFOutputStream() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final LZFOutputStream gzipOutputStream = new LZFOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_PLZFOutputStream() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final PLZFOutputStream gzipOutputStream = new PLZFOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_GzipCompressorOutputStream_DefaultCompressionLevel() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_GzipCompressorOutputStream_NoCompression() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final GzipParameters gzipParameters = new GzipParameters();
        gzipParameters.setCompressionLevel(Deflater.NO_COMPRESSION);

        try (final InputStream inputStream = resource.openStream();
             final GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(byteArrayOutputStream, gzipParameters)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_GzipCompressorOutputStream_FastestCompression() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final GzipParameters gzipParameters = new GzipParameters();
        gzipParameters.setCompressionLevel(Deflater.BEST_SPEED);

        try (final InputStream inputStream = resource.openStream();
             final GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(byteArrayOutputStream, gzipParameters)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_GzipCompressorOutputStream_BestCompression() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final GzipParameters gzipParameters = new GzipParameters();
        gzipParameters.setCompressionLevel(Deflater.BEST_COMPRESSION);

        try (final InputStream inputStream = resource.openStream();
             final GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(byteArrayOutputStream, gzipParameters)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_LZMACompressorOutputStream() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final LZMACompressorOutputStream gzipOutputStream = new LZMACompressorOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_FramedSnappyCompressorOutputStream() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final FramedSnappyCompressorOutputStream gzipOutputStream = new FramedSnappyCompressorOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_XZCompressorOutputStream_PresetDefault() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final XZCompressorOutputStream gzipOutputStream = new XZCompressorOutputStream(byteArrayOutputStream, LZMA2Options.PRESET_DEFAULT)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_XZCompressorOutputStream_PresetMin() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final XZCompressorOutputStream gzipOutputStream = new XZCompressorOutputStream(byteArrayOutputStream, LZMA2Options.PRESET_MIN)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }

    @Benchmark
    public int compressLargeJson_XZCompressorOutputStream_PresetMax() throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final XZCompressorOutputStream gzipOutputStream = new XZCompressorOutputStream(byteArrayOutputStream, LZMA2Options.PRESET_MAX)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        if (printSize) {
            System.out.println("size=" + byteArrayOutputStream.size());
        }
        return byteArrayOutputStream.size();
    }
}
