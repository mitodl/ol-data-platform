import random
import re

import polars as pl
import pytest
from ml.lib.courseware.tagging import (
    ConceptDictionary,
    _plural,
    _tokenize,
    _tokens,
    label_key,
    tag_sources,
    tagging_input_key,
)


def aliases(*rows):
    """Rows of (alias, concept_id); the key is normalized from the alias."""
    return pl.DataFrame(
        {
            "concept_label_key": [label_key(alias) for alias, _ in rows],
            "concept_fk": [concept for _, concept in rows],
            "alias": [alias for alias, _ in rows],
        }
    )


def sources(*texts):
    return pl.DataFrame(
        {
            "platform": ["mitxonline"] * len(texts),
            "courserun_readable_id": ["course-v1:X+Y+1T2026"] * len(texts),
            "scope_id": ["seq1"] * len(texts),
            "scope_kind": ["unit"] * len(texts),
            "source_ref": [f"block{index}" for index in range(len(texts))],
            "source_kind": ["html"] * len(texts),
            "source_text": list(texts),
        }
    )


def found(dictionary, text):
    return [
        (pattern.key, text[start:end]) for pattern, start, end in dictionary.find(text)
    ]


def test_matches_respect_token_boundaries():
    dictionary = ConceptDictionary(aliases(("art", "c1"), ("data frame", "c2")))
    assert found(dictionary, "smart metadata frames, art.") == [("art", "art")]


def test_longest_alias_wins_and_adjacent_mentions_are_both_found():
    dictionary = ConceptDictionary(
        aliases(("tree", "c1"), ("decision tree", "c2"), ("random forest", "c3"))
    )
    assert found(dictionary, "A decision tree random forest, a tree") == [
        ("decision tree", "decision tree"),
        ("random forest", "random forest"),
        ("tree", "tree"),
    ]


def test_matching_ignores_case_and_punctuation_between_words():
    dictionary = ConceptDictionary(aliases(("k-means clustering", "c1")))
    assert found(dictionary, "Use K-Means   Clustering here") == [
        ("k means clustering", "K-Means   Clustering")
    ]


def test_offsets_index_the_original_text():
    dictionary = ConceptDictionary(aliases(("naïve Bayes", "c1")))
    text = "Façade: a Naïve  Bayes model"
    [(_, start, end)] = dictionary.find(text)
    assert text[start:end] == "Naïve  Bayes"


def test_plural_variant_unless_it_is_a_label_of_its_own():
    dictionary = ConceptDictionary(
        aliases(("decision tree", "c1"), ("list", "c2"), ("lists", "c3"))
    )
    mentions = tag_sources(
        sources("decision trees and lists"),
        aliases(("decision tree", "c1"), ("list", "c2"), ("lists", "c3")),
    ).mentions
    assert found(dictionary, "decision trees and lists") == [
        ("decision trees", "decision trees"),
        ("lists", "lists"),
    ]
    assert mentions["concept_id"].to_list() == ["c1", "c3"]


def test_short_and_acronym_phrases_are_case_sensitive():
    dictionary = ConceptDictionary(
        aliases(("AUC", "c1"), ("CART", "c2"), ("Git", "c3"))
    )
    text = "auc AUC AUCs cart CART git Git GIT"
    assert found(dictionary, text) == [
        ("auc", "AUC"),
        ("aucs", "AUCs"),
        ("cart", "CART"),
        ("git", "Git"),
    ]


def test_too_short_aliases_are_skipped_but_recorded():
    result = tag_sources(sources("R and C"), aliases(("R", "c1")))
    assert result.mentions.is_empty()
    assert result.aliases["match_mode"].to_list() == ["skipped"]
    assert result.run["skipped_pattern_count"].item() == 1


def test_parentheticals_give_full_name_abbreviation_and_qualified_head():
    rows = aliases(
        ("mean squared error (MSE)", "c1"),
        ("None (special object)", "c2"),
        ("class (programming)", "c3"),
        ("class", "c4"),
        ("integer (int)", "c5"),
    )
    result = tag_sources(
        sources("The mean squared error, or MSE. none of it. None is a value. class"),
        rows,
    )
    variants = {
        row["pattern_key"]: (row["variant"], row["match_mode"], row["is_ambiguous"])
        for row in result.aliases.iter_rows(named=True)
    }
    assert variants["mean squared error"] == ("full_name", "caseless", False)
    assert variants["mse"] == ("abbreviation", "case_sensitive", True)
    assert variants["none"] == ("without_qualifier", "case_sensitive", True)
    # one lowercase word, so ambiguous on that count alone
    assert variants["integer"] == ("full_name", "caseless", True)
    assert variants["int"] == ("abbreviation", "case_sensitive", True)
    # an explicit label keeps its own assignment over another label's head
    assert result.aliases.filter(pl.col("pattern_key") == "class")[
        "concept_id"
    ].to_list() == ["c4"]
    # a lowercase parenthetical is a qualifier, not an abbreviation
    assert "programming" not in variants
    assert result.mentions.select("matched_text", "concept_id").rows() == [
        ("mean squared error", "c1"),
        ("MSE", "c1"),
        ("None", "c2"),
        ("class", "c4"),
    ]


def test_phrase_shared_by_concepts_is_ambiguous_and_tags_each():
    rows = aliases(
        ("comma-separated values (CSV)", "c1"),
        ("CSV file (CSV)", "c2"),
    )
    result = tag_sources(sources("Load the CSV."), rows)
    assert sorted(result.mentions["concept_id"].to_list()) == ["c1", "c2"]
    csv = result.aliases.filter(pl.col("pattern_key") == "csv")
    assert csv["has_multiple_concepts"].to_list() == [True, True]


def test_single_lowercase_words_are_flagged_not_dropped():
    result = tag_sources(
        sources("The data and Python."), aliases(("data", "c1"), ("Python", "c2"))
    )
    flags = dict(result.aliases.select("pattern_key", "is_single_word").iter_rows())
    assert flags == {"data": True, "datas": True, "python": False, "pythons": False}
    assert result.mentions["matched_text"].to_list() == ["data", "Python"]


def test_run_record_and_input_key():
    rows = aliases(("decision tree", "c1"))
    texts = sources("a decision tree", "nothing here", "")
    result = tag_sources(texts, rows)
    run = result.run.row(0, named=True)
    assert (run["source_count"], run["tagged_source_count"], run["mention_count"]) == (
        3,
        1,
        1,
    )
    assert run["char_count"] == len("a decision tree") + len("nothing here")
    assert run["input_key"] == tagging_input_key(ConceptDictionary(rows), texts)
    assert result.mentions["tagging_run_id"].unique().to_list() == [
        run["tagging_run_id"]
    ]
    # every run gets its own id, so re-running old inputs adds a new newest run
    assert (
        tag_sources(texts, rows).run["tagging_run_id"].item() != run["tagging_run_id"]
    )
    changed = aliases(("decision tree", "c1"), ("tree", "c2"))
    assert tagging_input_key(ConceptDictionary(changed), texts) != run["input_key"]


def test_rejected_case_sensitive_match_does_not_hide_shorter_ones():
    dictionary = ConceptDictionary(
        aliases(("tree", "c1"), ("Decision tree (graph theory)", "c2"))
    )
    assert found(dictionary, "a decision tree splits") == [("tree", "tree")]
    assert found(dictionary, "a Decision tree splits") == [
        ("decision tree", "Decision tree")
    ]


@pytest.mark.parametrize(
    ("label", "variants"),
    [
        (
            "mean squared error (MSE)",
            {"mean squared error": "full_name", "mse": "abbreviation"},
        ),
        (
            "area under the ROC curve (AUC)",
            {"area under the roc curve": "full_name", "auc": "abbreviation"},
        ),
        (
            "classification and regression tree (CART)",
            {"classification and regression tree": "full_name", "cart": "abbreviation"},
        ),
        (
            "locally weighted scatterplot smoothing (LOWESS)",
            {
                "locally weighted scatterplot smoothing": "full_name",
                "lowess": "abbreviation",
            },
        ),
        ("integer (int)", {"integer": "full_name", "int": "abbreviation"}),
        ("bias (ML)", {"bias": "without_qualifier"}),
        ("bias (SVM)", {"bias": "without_qualifier"}),
        ("Mercury (god)", {"mercury": "without_qualifier"}),
        ("class (programming)", {"class": "without_qualifier"}),
    ],
)
def test_parenthetical_is_an_abbreviation_only_when_it_abbreviates_the_head(
    label, variants
):
    result = tag_sources(sources("x"), aliases((label, "c1")))
    found_variants = {
        row["pattern_key"]: row["variant"]
        for row in result.aliases.iter_rows(named=True)
        if row["variant"] not in ("label", "plural")
    }
    assert found_variants == variants


def test_full_name_from_one_label_unqualifies_the_phrase():
    result = tag_sources(
        sources("x"),
        aliases(
            ("coefficient of determination (COD)", "c1"),
            ("coefficient of determination (R squared)", "c1"),
        ),
    )
    row = result.aliases.filter(
        pl.col("pattern_key") == "coefficient of determination"
    ).row(0, named=True)
    assert (row["variant"], row["is_qualified"], row["match_mode"]) == (
        "full_name",
        False,
        "caseless",
    )


def test_empty_parentheses_are_ignored():
    result = tag_sources(sources("x"), aliases(("file modes in open()", "c1")))
    assert result.aliases["pattern_key"].to_list() == ["file modes in open"]


def test_plurals_inherit_case_sensitivity():
    dictionary = ConceptDictionary(aliases(("Git", "c1"), ("Map", "c2")))
    assert found(dictionary, "gits Gits maps Maps") == [
        ("gits", "Gits"),
        ("maps", "Maps"),
    ]
    flags = {
        pattern.key: (pattern.match_mode, pattern.is_ambiguous)
        for pattern in dictionary.patterns
    }
    assert flags["gits"] == ("case_sensitive", True)


def test_non_ascii_letters_are_part_of_the_word():
    dictionary = ConceptDictionary(
        aliases(("int", "c1"), ("ve", "c2"), ("naïve Bayes", "c3"))
    )
    assert found(dictionary, "intérieur, naïve, int, a naïve Bayes model") == [
        ("int", "int"),
        ("naïve bayes", "naïve Bayes"),
    ]


def test_labels_without_a_letter_or_number_are_skipped_and_counted():
    result = tag_sources(
        sources("∇, then the variance"),
        aliases(("∇", "c1"), ("∂", "c3"), ("variance", "c2")),
    )
    assert result.mentions["concept_id"].to_list() == ["c2"]
    assert set(result.aliases["concept_id"]) == {"c2"}
    # the null key of the tokenless labels is not a label in the dictionary
    assert result.run["label_count"].item() == 1
    assert result.run["tokenless_label_count"].item() == 2


@pytest.mark.parametrize("label", [".", "..", ".+", "+."])
def test_symbol_only_labels_have_no_key_and_are_skipped(label):
    # spelled out ("dot", "dot plus"), "dot" alone would tag every sentence after a
    # stray period, and the word "dot" in "dot plot"
    assert label_key(label) is None
    result = tag_sources(
        sources(". The dot plot shows variance. .. and .+ too"),
        aliases((label, "c1"), ("variance", "c2")),
    )
    assert result.mentions["concept_id"].to_list() == ["c2"]
    assert result.run["tokenless_label_count"].item() == 1


def test_plural_needs_a_word_at_the_end():
    assert _plural(".+") is None
    assert _plural("C++") is None
    assert _plural("decision tree") == "decision trees"


def test_non_ascii_letters_are_part_of_their_token():
    dictionary = ConceptDictionary(
        aliases(("Fiancé", "c1"), ("Éclair", "c2"), ("Schrödinger equation", "c3"))
    )
    text = (
        "Fiancé, fiancé, fianc, fiancée; an Éclair, a clair; the Schrödinger "
        "equation, not the Schrodinger equation"
    )
    assert found(dictionary, text) == [
        ("fiancé", "Fiancé"),
        ("fiancé", "fiancé"),
        ("éclair", "Éclair"),
        ("schrödinger equation", "Schrödinger equation"),
    ]


def test_non_ascii_letters_between_tokens_must_be_the_aliases_own():
    dictionary = ConceptDictionary(aliases(("naïve Bayes", "c1")))
    assert found(dictionary, "naïve Bayes, na ve Bayes, naüve Bayes, NAÏVE BAYES") == [
        ("naïve bayes", "naïve Bayes"),
        ("naïve bayes", "NAÏVE BAYES"),
    ]


def test_a_non_ascii_lowercase_letter_rules_out_an_acronym():
    dictionary = ConceptDictionary(aliases(("NAïVE", "c1")))
    [pattern] = [p for p in dictionary.patterns if p.key == "naïve"]
    assert pattern.match_mode == "caseless"


def test_spelled_out_symbols_match_the_text_that_has_them():
    dictionary = ConceptDictionary(
        aliases(("C++", "c1"), ("C#", "c2"), (".NET", "c3"), ("F# language", "c4"))
    )
    text = "C++ and C# on .NET; not C + or # of rows; the F# language; ASP.NET"
    assert found(dictionary, text) == [
        ("c plus plus", "C++"),
        ("c sharp", "C#"),
        ("dot net", ".NET"),
        ("f sharp language", "F# language"),
    ]


def _sql_label_key(label):
    """Apply the courseware_label_key macro's check and passes, one for one."""
    if not re.search(r"[^\W_]", label):
        return None
    key = label.lower()
    key = re.sub(r"^[+#]", " ", key)
    key = re.sub(r"[ \t\n\f\r][+#]", " ", key)
    key = re.sub(r"[+]", " plus ", key)
    key = re.sub(r"#", " sharp ", key)
    key = re.sub(r"^[.]", "dot ", key)
    key = re.sub(r"[ \t\n\f\r][.]", " dot ", key)
    return re.sub(r"[\W_]+", " ", key).strip()


# the macro's unit-test cases (_courseware_label_key__unit_tests.yml), then edge cases
LABEL_KEY_CASES = [
    ("C", "c"),
    ("C++", "c plus plus"),
    ("C#", "c sharp"),
    (".NET", "dot net"),
    ("# of rows", "of rows"),
    ("adjusted R²", "adjusted r²"),
    ("K-Means  Clustering!", "k means clustering"),
    ("+", None),
    (".", None),
    ("..", None),
    (".+", None),
    ("#.", None),
    ("F#", "f sharp"),
    ("ASP.NET", "asp net"),
    ("the .csv file", "the dot csv file"),
    ("Schrödinger", "schrödinger"),
    ("a + b", "a b"),
    ("++x", "x"),
    ("a ++b", "a plus b"),
    ("c+.net", "c plus dot net"),
    ("x +# y", "x sharp y"),
    ("..net", "dot dot net"),
    ("snake_case name", "snake case name"),
    ("∇", None),
    ("#", None),
    ("+.", None),
    (". .", None),
    ("İstanbul", "i stanbul"),
    ("Naïve Bayes", "naïve bayes"),
]


@pytest.mark.parametrize(("label", "key"), LABEL_KEY_CASES)
def test_label_key_matches_the_courseware_label_key_macro(label, key):
    assert label_key(label) == key
    assert _sql_label_key(label) == key


def test_text_tokens_join_to_the_label_key():
    alphabet = "aZ9ö²İ_ .+#-\t,"
    rng = random.Random(0)  # noqa: S311 - fixed fuzz inputs, not security
    texts = [label for label, _ in LABEL_KEY_CASES] + [
        "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12)))
        for _ in range(5000)
    ]
    for text in texts:
        tokens = _tokens(text)
        joined = " ".join(low for low, _, _, _ in tokens)
        if label_key(text) is None:
            # symbols are still spelled out in text, though such a label has no key
            assert set(joined.split()) <= {"dot", "plus", "sharp"}, text
        else:
            assert joined == label_key(text), text
        # the fast path gives the same haystacks and spans as the full tokenizer
        tokenized = _tokenize(text)
        if not tokens:
            assert tokenized is None, text
            continue
        assert tokenized.caseless == f" {'  '.join(t[0] for t in tokens)} ", text
        assert tokenized.exact == f" {'  '.join(t[1] for t in tokens)} ", text
        assert tokenized.starts.tolist() == [t[2] for t in tokens], text
        assert tokenized.ends.tolist() == [t[3] for t in tokens], text


@pytest.mark.parametrize(
    ("label", "text", "surface"),
    [
        # casefold turns "ß" into "ss" and "İ" into "i" plus a combining dot
        ("Strauß", "a Strauß waltz", "Strauß"),
        ("İstanbul", "in İstanbul today", "İstanbul"),
        ("Großmann method", "the GROßMANN METHOD works", "GROßMANN METHOD"),
    ],
)
def test_offsets_hold_when_casefolding_changes_a_letters_length(label, text, surface):
    result = tag_sources(sources(text), aliases((label, "c1")))
    [mention] = result.mentions.iter_rows(named=True)
    assert text[mention["char_start"] : mention["char_end"]] == surface
    assert mention["matched_text"] == surface
