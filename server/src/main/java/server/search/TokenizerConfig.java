package server.search;

import opennlp.tools.tokenize.DetokenizationDictionary.Operation;

public class TokenizerConfig {
    static String TOKENIZER_URL = "https://dlcdn.apache.org/opennlp/models/ud-models-1.0/opennlp-en-ud-ewt-tokens-1.0-1.9.3.bin";

    static String[] SPECIAL_TOKENS = {
        ".",
        ",", 
        "?",
        "!",
        ":",
        ";",
        "\"",
        "'",
        "(",
        ")",
        "[",
        "]",
        "{",
        "}"
    };

    static Operation[] DETOKENIZE_RULES = {
        Operation.MOVE_LEFT,
        Operation.MOVE_LEFT,
        Operation.MOVE_LEFT,
        Operation.MOVE_LEFT,
        Operation.MOVE_LEFT,
        Operation.MOVE_LEFT,
        Operation.RIGHT_LEFT_MATCHING,
        Operation.RIGHT_LEFT_MATCHING,
        Operation.MOVE_RIGHT,
        Operation.MOVE_LEFT,
        Operation.MOVE_RIGHT,
        Operation.MOVE_LEFT,
        Operation.MOVE_RIGHT,
        Operation.MOVE_LEFT
    };
}
