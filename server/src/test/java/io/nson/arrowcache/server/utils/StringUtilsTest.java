package io.nson.arrowcache.server.utils;

import io.nson.arrowcache.common.utils.StringUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class StringUtilsTest {
    private static final char SEP = '|';

    @Test
    public void testEmptyString() {
        assertIterableEquals(List.of(""), StringUtils.split("", SEP));
    }

    @Test
    public void testJustSep() {
        assertIterableEquals(List.of("", ""), StringUtils.split("|", SEP));
    }

    @Test
    public void testSepAtStart() {
        assertIterableEquals(List.of("", "b"), StringUtils.split("|b", SEP));
    }

    @Test
    public void testSepAtEnd() {
        assertIterableEquals(List.of("a", ""), StringUtils.split("a|", SEP));
    }

    @Test
    public void testTwoParts() {
        assertIterableEquals(List.of("a", "b"), StringUtils.split("a|b", SEP));
    }

    @Test
    public void testDoubleSepAtStart() {
        assertIterableEquals(List.of("", "", "b"), StringUtils.split("||b", SEP));
    }

    @Test
    public void testDoubleSep() {
        assertIterableEquals(List.of("a", "", "b"), StringUtils.split("a||b", SEP));
    }

    @Test
    public void testDoubleSepAtEnd() {
        assertIterableEquals(List.of("a", "", ""), StringUtils.split("a||", SEP));
    }

    @Test
    public void testRandomChars() {
        assertIterableEquals(List.of("a!\"£$", "b@~><"), StringUtils.split("a!\"£$|b@~><", SEP));
    }
}
