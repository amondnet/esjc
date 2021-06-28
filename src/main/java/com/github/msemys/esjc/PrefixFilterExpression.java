package com.github.msemys.esjc;

import java.util.Objects;
import org.jetbrains.annotations.NotNull;

public class PrefixFilterExpression implements Comparable<PrefixFilterExpression> {
    public static PrefixFilterExpression NONE = new PrefixFilterExpression("");

    @NotNull
    private final String value;

    public PrefixFilterExpression(@NotNull String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrefixFilterExpression that = (PrefixFilterExpression) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public int compareTo(PrefixFilterExpression other) {
        return this.value.compareTo(other.value);
    }
}