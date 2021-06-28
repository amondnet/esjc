package com.github.msemys.esjc;

import java.util.Optional;

public interface EventFilter {
    PrefixFilterExpression[] getPrefixFilterExpressions();

    RegularFilterExpression getRegularFilterExpression();

    Optional<Integer> getMaxSearchWindow();
}