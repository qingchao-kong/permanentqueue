package cn.pockethub.permanentqueue.kafka.utils;

import org.apache.commons.collections4.CollectionUtils;

import java.util.*;

public final class CollectionUtilExt {

    private CollectionUtilExt() {
    }

    public static <T> List<T> dropRight(List<T> source, int n) {
        if (CollectionUtils.isEmpty(source) || n < 0 || source.size() == n) {
            return new ArrayList<>(0);
        }

        int startIndex = Math.max(source.size() - n, 0);
        return source.subList(0, startIndex);
    }

    public static <T> List<T> tail(List<T> source) {
        if (CollectionUtils.isEmpty(source)) {
            throw new UnsupportedOperationException();
        }
        return source.subList(1, source.size());
    }

    public static <T> T last(Iterable<T> source) {
        if (Objects.isNull(source)) {
            throw new NoSuchElementException("Utils.List.last");
        }
        Iterator<T> iterator = source.iterator();
        T last = null;
        while (iterator.hasNext()) {
            last = iterator.next();
        }
        if (Objects.isNull(last)) {
            throw new NoSuchElementException("Utils.List.last");
        }
        return last;
    }

    public static <T> T head(Iterable<T> source) {
        if (Objects.isNull(source)) {
            throw new NoSuchElementException("Utils.Iterable.head");
        }
        Iterator<T> iterator = source.iterator();
        if (!iterator.hasNext()) {
            throw new NoSuchElementException("Utils.Iterable.head");
        }
        return iterator.next();
    }

    public static <T> String mkString(Iterable<T> source, String sep) {
        StringBuilder builder = new StringBuilder("[");
        Iterator<T> iterator = source.iterator();
        while (iterator.hasNext()) {
            builder.append(iterator.next().toString());
            if (iterator.hasNext()) {
                builder.append(sep);
            }
        }
        builder.append("]");
        return builder.toString();
    }

    public static <T> Optional<T> lastOptional(Iterable<T> source) {
        if (Objects.isNull(source)) {
            return Optional.empty();
        }
        T last = null;
        for (T t : source) {
            last = t;
        }

        return Optional.ofNullable(last);
    }

    public static <T> int size(Iterable<T> source) {
        int size = 0;
        for (T t : source) {
            size++;
        }
        return size;
    }
}
