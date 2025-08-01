/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.VocabularyImpl;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.BitSet;
import java.util.EmptyStackException;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.core.util.StringUtils.isInteger;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.nameOrPosition;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;

public class EsqlParser {

    private static final Logger log = LogManager.getLogger(EsqlParser.class);

    /**
     * Maximum number of characters in an ESQL query. Antlr may parse the entire
     * query into tokens to make the choices, buffering the world. There's a lot we
     * can do in the grammar to prevent that, but let's be paranoid and assume we'll
     * fail at preventing antlr from slurping in the world. Instead, let's make sure
     * that the world just isn't that big.
     */
    public static final int MAX_LENGTH = 1_000_000;

    private static void replaceSymbolWithLiteral(Map<String, String> symbolReplacements, String[] literalNames, String[] symbolicNames) {
        for (int i = 0, replacements = symbolReplacements.size(); i < symbolicNames.length && replacements > 0; i++) {
            String symName = symbolicNames[i];
            if (symName != null) {
                String replacement = symbolReplacements.get(symName);
                if (replacement != null && literalNames[i] == null) {
                    // literals are single quoted
                    literalNames[i] = "'" + replacement + "'";
                    replacements--;
                }
            }
        }
    }

    /**
     * Add the literal name to a number of tokens that due to ANTLR internals/ATN
     * have their symbolic name returns instead during error reporting.
     * When reporting token errors, ANTLR uses the Vocabulary class to get the displayName
     * (if set), otherwise falls back to the literal one and eventually uses the symbol name.
     * Since the Vocabulary is static and not pluggable, this code modifies the underlying
     * arrays by setting the literal string manually based on the token index.
     * This is needed since some symbols, especially around setting up the mode, end up losing
     * their literal representation.
     * NB: this code is highly dependent on the ANTLR internals and thus will likely break
     * during upgrades.
     * NB: Can't use this for replacing DEV_ since the Vocabular is static while DEV_ replacement occurs per runtime configuration
     */
    static {
        Map<String, String> symbolReplacements = Map.of("LP", "(", "OPENING_BRACKET", "[");

        // the vocabularies have the same content however are different instances
        // for extra reliability, perform the replacement for each map
        VocabularyImpl parserVocab = (VocabularyImpl) EsqlBaseParser.VOCABULARY;
        replaceSymbolWithLiteral(symbolReplacements, parserVocab.getLiteralNames(), parserVocab.getSymbolicNames());

        VocabularyImpl lexerVocab = (VocabularyImpl) EsqlBaseLexer.VOCABULARY;
        replaceSymbolWithLiteral(symbolReplacements, lexerVocab.getLiteralNames(), lexerVocab.getSymbolicNames());
    }

    private EsqlConfig config = new EsqlConfig();

    public EsqlConfig config() {
        return config;
    }

    public void setEsqlConfig(EsqlConfig config) {
        this.config = config;
    }

    // testing utility
    public LogicalPlan createStatement(String query, Configuration configuration) {
        return createStatement(query, new QueryParams(), configuration);
    }

    // testing utility
    public LogicalPlan createStatement(String query, QueryParams params, Configuration configuration) {
        return createStatement(query, params, new PlanTelemetry(new EsqlFunctionRegistry()), configuration);
    }

    public LogicalPlan createStatement(String query, QueryParams params, PlanTelemetry metrics, Configuration configuration) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as statement: {}", query);
        }
        return invokeParser(query, params, metrics, EsqlBaseParser::singleStatement, AstBuilder::plan, configuration);
    }

    private <T> T invokeParser(
        String query,
        QueryParams params,
        PlanTelemetry metrics,
        Function<EsqlBaseParser, ParserRuleContext> parseFunction,
        BiFunction<AstBuilder, ParserRuleContext, T> result,
        Configuration configuration
    ) {
        if (query.length() > MAX_LENGTH) {
            throw new ParsingException("ESQL statement is too large [{} characters > {}]", query.length(), MAX_LENGTH);
        }
        try {
            EsqlBaseLexer lexer = new EsqlBaseLexer(CharStreams.fromString(query));

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            lexer.setEsqlConfig(config);

            TokenSource tokenSource = new ParametrizedTokenSource(lexer, params);
            CommonTokenStream tokenStream = new CommonTokenStream(tokenSource);
            EsqlBaseParser parser = new EsqlBaseParser(tokenStream);

            parser.addParseListener(new PostProcessor());

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

            parser.setEsqlConfig(config);

            ParserRuleContext tree = parseFunction.apply(parser);

            if (log.isTraceEnabled()) {
                log.trace("Parse tree: {}", tree.toStringTree());
            }

            return result.apply(new AstBuilder(new ExpressionBuilder.ParsingContext(params, metrics)), tree);
        } catch (StackOverflowError e) {
            throw new ParsingException("ESQL statement is too large, causing stack overflow when generating the parsing tree: [{}]", query);
            // likely thrown by an invalid popMode (such as extra closing parenthesis)
        } catch (EmptyStackException ese) {
            throw new ParsingException("Invalid query [{}]", query);
        }
    }

    private class PostProcessor extends EsqlBaseParserBaseListener {
        @Override
        public void exitFunctionExpression(EsqlBaseParser.FunctionExpressionContext ctx) {
            // TODO remove this at some point
            EsqlBaseParser.FunctionNameContext identifier = ctx.functionName();
            if (identifier.getText().equalsIgnoreCase("is_null")) {
                throw new ParsingException(
                    source(ctx),
                    "is_null function is not supported anymore, please use 'is null'/'is not null' predicates instead"
                );
            }
        }
    }

    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
        // replace entries that start with <comma?><space?>DEV_<space?>
        private final Pattern REPLACE_DEV = Pattern.compile(",*\\s*DEV_\\w+\\s*");

        @Override
        public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPositionInLine,
            String message,
            RecognitionException e
        ) {
            if (recognizer instanceof EsqlBaseParser parser) {
                Matcher m;

                if (parser.isDevVersion() == false) {
                    m = REPLACE_DEV.matcher(message);
                    message = m.replaceAll(StringUtils.EMPTY);
                }
            }
            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };

    /**
     * Finds all parameter tokens (?) and associates them with actual parameter values.
     * <p>
     * Parameters are positional and we know where parameters occurred in the original stream in order to associate them
     * with actual values.
     */
    private static class ParametrizedTokenSource extends DelegatingTokenSource {
        private static String message = "Inconsistent parameter declaration, "
            + "use one of positional, named or anonymous params but not a combination of ";

        private QueryParams params;
        private BitSet paramTypes = new BitSet(3);
        private int param = 1;

        ParametrizedTokenSource(TokenSource delegate, QueryParams params) {
            super(delegate);
            this.params = params;
        }

        @Override
        public Token nextToken() {
            Token token = delegate.nextToken();
            if (token.getType() == EsqlBaseLexer.PARAM || token.getType() == EsqlBaseLexer.DOUBLE_PARAMS) {
                checkAnonymousParam(token);
                if (param > params.size()) {
                    throw new ParsingException(source(token), "Not enough actual parameters {}", params.size());
                }
                params.addTokenParam(token, params.get(param));
                param++;
            }

            String nameOrPosition = nameOrPosition(token);
            if (nameOrPosition.isBlank() == false) {
                if (isInteger(nameOrPosition)) {
                    checkPositionalParam(token);
                } else {
                    checkNamedParam(token);
                }
            }
            return token;
        }

        private void checkAnonymousParam(Token token) {
            paramTypes.set(0);
            if (paramTypes.cardinality() > 1) {
                throw new ParsingException(source(token), message + "anonymous and " + (paramTypes.get(1) ? "named" : "positional"));
            }
        }

        private void checkNamedParam(Token token) {
            paramTypes.set(1);
            if (paramTypes.cardinality() > 1) {
                throw new ParsingException(source(token), message + "named and " + (paramTypes.get(0) ? "anonymous" : "positional"));
            }
        }

        private void checkPositionalParam(Token token) {
            paramTypes.set(2);
            if (paramTypes.cardinality() > 1) {
                throw new ParsingException(source(token), message + "positional and " + (paramTypes.get(0) ? "anonymous" : "named"));
            }
        }
    }
}
