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
import org.openjdk.jmh.annotations.AuxCounters;
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
    private final URL resource = Resources.getResource("large2.json");

    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class CompressedSize {
        public long compressedBytes;
    }

    @Benchmark
    public void compressLargeJson_GZIPOutputStream_DefaultBufferSize(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_GZIPOutputStream_BufferSize8192(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream, 8192)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_GZIPOutputStream_BufferedOutputStream(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
             final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(gzipOutputStream)) {
            ByteStreams.copy(inputStream, bufferedOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_ParallelGZIPOutputStream(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final ParallelGZIPOutputStream gzipOutputStream = new ParallelGZIPOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_OptimizedGZIPOutputStream(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final OptimizedGZIPOutputStream gzipOutputStream = new OptimizedGZIPOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_LZFOutputStream(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final LZFOutputStream gzipOutputStream = new LZFOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_PLZFOutputStream(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final PLZFOutputStream gzipOutputStream = new PLZFOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_GzipCompressorOutputStream_DefaultCompressionLevel(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_GzipCompressorOutputStream_NoCompression(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final GzipParameters gzipParameters = new GzipParameters();
        gzipParameters.setCompressionLevel(Deflater.NO_COMPRESSION);

        try (final InputStream inputStream = resource.openStream();
             final GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(byteArrayOutputStream, gzipParameters)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_GzipCompressorOutputStream_FastestCompression(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final GzipParameters gzipParameters = new GzipParameters();
        gzipParameters.setCompressionLevel(Deflater.BEST_SPEED);

        try (final InputStream inputStream = resource.openStream();
             final GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(byteArrayOutputStream, gzipParameters)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_GzipCompressorOutputStream_BestCompression(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        final GzipParameters gzipParameters = new GzipParameters();
        gzipParameters.setCompressionLevel(Deflater.BEST_COMPRESSION);

        try (final InputStream inputStream = resource.openStream();
             final GzipCompressorOutputStream gzipOutputStream = new GzipCompressorOutputStream(byteArrayOutputStream, gzipParameters)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_LZMACompressorOutputStream(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final LZMACompressorOutputStream gzipOutputStream = new LZMACompressorOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_FramedSnappyCompressorOutputStream(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final FramedSnappyCompressorOutputStream gzipOutputStream = new FramedSnappyCompressorOutputStream(byteArrayOutputStream)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_XZCompressorOutputStream_PresetDefault(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final XZCompressorOutputStream gzipOutputStream = new XZCompressorOutputStream(byteArrayOutputStream, LZMA2Options.PRESET_DEFAULT)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_XZCompressorOutputStream_PresetMin(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final XZCompressorOutputStream gzipOutputStream = new XZCompressorOutputStream(byteArrayOutputStream, LZMA2Options.PRESET_MIN)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }

    @Benchmark
    public void compressLargeJson_XZCompressorOutputStream_PresetMax(CompressedSize compressedSize) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        try (final InputStream inputStream = resource.openStream();
             final XZCompressorOutputStream gzipOutputStream = new XZCompressorOutputStream(byteArrayOutputStream, LZMA2Options.PRESET_MAX)) {
            ByteStreams.copy(inputStream, gzipOutputStream);
        }

        compressedSize.compressedBytes = byteArrayOutputStream.size();
    }
}
