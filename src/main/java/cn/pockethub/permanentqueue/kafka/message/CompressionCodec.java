package cn.pockethub.permanentqueue.kafka.message;

import cn.pockethub.permanentqueue.kafka.common.UnknownCodecException;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public enum CompressionCodec {
    NoCompressionCodec(0, "none"),
    GZIPCompressionCodec(1, "gzip"),
    SnappyCompressionCodec(2, "snappy"),
    LZ4CompressionCodec(3, "lz4"),
    ZStdCompressionCodec(4, "zstd"),
    UncompressedCodec(5, "uncompressed"),
    ProducerCompressionCodec(6, "producer"),
    DefaultCompressionCodec(GZIPCompressionCodec.codec, GZIPCompressionCodec.name),
    ;

    private final int codec;
    private final String name;

    private CompressionCodec(int codec, String name) {
        this.codec = codec;
        this.name = name;
    }

    public int getCodec() {
        return this.codec;
    }

    public String getName() {
        return this.name;
    }


    public static CompressionCodec fromCodec(int codec) {
        switch (codec) {
            case 0:
                return NoCompressionCodec;
            case 1:
                return GZIPCompressionCodec;
            case 2:
                return SnappyCompressionCodec;
            case 3:
                return LZ4CompressionCodec;
            case 4:
                return ZStdCompressionCodec;
            default:
                throw new UnknownCodecException(String.format("%d is an unknown compression codec", codec));
        }
    }

    private static CompressionCodec fromName(String name) {
        switch (name.toLowerCase()) {
            case "none":
                return NoCompressionCodec;
            case "gzip":
                return GZIPCompressionCodec;
            case "snappy":
                return SnappyCompressionCodec;
            case "lz4":
                return LZ4CompressionCodec;
            case "uncompressed":
                return UncompressedCodec;
            case "producer":
                return ProducerCompressionCodec;
            default:
                throw new UnknownCodecException(String.format("%s is an unknown compression codec", name));
        }
    }


    public static CompressionCodec getCompressionCodec(final int codec) {
        return fromCodec(codec);
    }

    public static CompressionCodec getCompressionCodec(final String codec) {
        return fromName(codec);
    }

    public static class BrokerCompressionCodec {
        public static final List<CompressionCodec> brokerCompressionCodecs = Arrays.asList(UncompressedCodec,
                ZStdCompressionCodec,
                LZ4CompressionCodec,
                SnappyCompressionCodec,
                GZIPCompressionCodec,
                ProducerCompressionCodec);

        public static final List<String> brokerCompressionOptions = brokerCompressionCodecs.stream()
                .map(codec -> codec.name)
                .collect(Collectors.toList());

        public static Boolean isValid(String compressionType) {
            return brokerCompressionOptions.contains(compressionType.toLowerCase(Locale.ROOT));
        }

        public static CompressionCodec getCompressionCodec(String compressionType) {
            if (compressionType.toLowerCase(Locale.ROOT).equals(UncompressedCodec.name)) {
                return NoCompressionCodec;
            } else {
                return CompressionCodec.getCompressionCodec(compressionType);
            }
        }

        public static CompressionCodec getTargetCompressionCodec(String compressionType, CompressionCodec producerCompression) {
            if (ProducerCompressionCodec.name.equals(compressionType)) {
                return producerCompression;
            } else {
                return getCompressionCodec(compressionType);
            }
        }
    }
}


