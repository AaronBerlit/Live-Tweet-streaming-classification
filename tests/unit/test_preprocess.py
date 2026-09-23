"""Preprocessing transforms, including the adversarial inputs Phase 2 requires.

PRD Phase 2 gate: "passes including adversarial inputs (empty, URL-only,
emoji-only, 5000 chars, RTL text)". Those are the inputs that crash or
silently corrupt a naive implementation, and every one of them is in the real
corpus.
"""

from __future__ import annotations

import pytest

from pipeline import preprocess
from pipeline.emoji_lexicon import NEGATIVE_TOKEN, NEUTRAL_TOKEN, POSITIVE_TOKEN

pytestmark = pytest.mark.spark

SCHEMA = "text string, user string"


def frame(spark, rows):
    """Build a one- or many-row DataFrame with the corpus's two text columns."""
    return spark.createDataFrame(rows, SCHEMA)


def cleaned(spark, text, **kwargs):
    df = frame(spark, [(text, "someone")])
    return preprocess.clean_text(df, **kwargs).first()[preprocess.TEXT_CLEAN]


# --- clean_text --------------------------------------------------------------


def test_lowercases(spark):
    assert cleaned(spark, "Hello WORLD") == "hello world"


def test_strips_urls(spark):
    assert cleaned(spark, "check http://t.co/abc123 now") == "check now"


def test_strips_https_and_www_urls(spark):
    assert cleaned(spark, "see https://example.com/x and www.example.org/y") == "see and"


def test_strips_mentions(spark):
    assert cleaned(spark, "thanks @alice and @bob_42") == "thanks and"


def test_preserves_hashtag_hash(spark):
    """C21 depends on this: stripping '#' here silently breaks hashtag analysis."""
    assert cleaned(spark, "great #Election results") == "great #election results"


def test_collapses_whitespace(spark):
    assert cleaned(spark, "too    many\t\tspaces   here") == "too many spaces here"


def test_trims_leading_and_trailing_whitespace(spark):
    assert cleaned(spark, "   padded   ") == "padded"


# --- adversarial inputs ------------------------------------------------------


def test_empty_string(spark):
    assert cleaned(spark, "") == ""


def test_whitespace_only(spark):
    assert cleaned(spark, "      \t  \n ") == ""


def test_null_text_becomes_empty_not_null(spark):
    """Null must not propagate: downstream transforms would all need to
    special-case it, and one of them would eventually forget."""
    df = spark.createDataFrame([(None, "someone")], SCHEMA)
    assert preprocess.clean_text(df).first()[preprocess.TEXT_CLEAN] == ""


def test_url_only_record_cleans_to_empty(spark):
    assert cleaned(spark, "http://t.co/onlyalink") == ""


def test_mention_only_record_cleans_to_empty(spark):
    assert cleaned(spark, "@someone @someoneelse") == ""


def test_five_thousand_characters(spark):
    text = "spam " * 1000
    result = cleaned(spark, text)
    assert result.startswith("spam spam")
    assert len(result) == len("spam " * 1000) - 1  # trailing space trimmed


def test_rtl_text_is_preserved(spark):
    """Arabic script must survive unchanged -- it is not noise to be stripped."""
    arabic = "مرحبا بالعالم"
    assert cleaned(spark, arabic) == arabic


def test_embedded_newlines_collapse_to_single_spaces(spark):
    assert cleaned(spark, "line one\nline two\r\nline three") == "line one line two line three"


def test_text_that_is_only_punctuation_survives(spark):
    assert cleaned(spark, "!!! ???") == "!!! ???"


# --- emoji strategies --------------------------------------------------------

HAPPY = "\U0001F600"
SAD = "\U0001F622"
OBSCURE = "\U0001F6F8"  # flying saucer: in no sentiment list


def test_emoji_strip_removes_them(spark):
    assert cleaned(spark, f"great day {HAPPY}", emoji_strategy="strip") == "great day"


def test_emoji_keep_leaves_them(spark):
    result = cleaned(spark, f"great day {HAPPY}", emoji_strategy="keep")
    assert HAPPY in result


def test_emoji_map_replaces_with_sentiment_tokens(spark):
    result = cleaned(spark, f"great {HAPPY} awful {SAD}", emoji_strategy="map")
    assert POSITIVE_TOKEN in result
    assert NEGATIVE_TOKEN in result


def test_unlisted_emoji_maps_to_neutral_rather_than_vanishing(spark):
    """Coverage gaps in the lexicon degrade to neutral; they do not delete data."""
    result = cleaned(spark, f"look {OBSCURE}", emoji_strategy="map")
    assert NEUTRAL_TOKEN in result


def test_emoji_only_record_under_strip_becomes_empty(spark):
    assert cleaned(spark, f"{HAPPY}{SAD}", emoji_strategy="strip") == ""


def test_emoji_only_record_under_map_becomes_tokens(spark):
    result = cleaned(spark, f"{HAPPY}{SAD}", emoji_strategy="map")
    assert POSITIVE_TOKEN in result and NEGATIVE_TOKEN in result


def test_unknown_emoji_strategy_is_rejected(spark):
    with pytest.raises(ValueError, match="emoji strategy"):
        cleaned(spark, "anything", emoji_strategy="sometimes")


# --- hashtags ----------------------------------------------------------------


def hashtags_of(spark, text):
    df = preprocess.clean_text(frame(spark, [(text, "u")]))
    return preprocess.extract_hashtags(df).first()[preprocess.HASHTAGS]


def test_extracts_hashtags_lowercased_without_hash(spark):
    assert hashtags_of(spark, "#Election #Results today") == ["election", "results"]


def test_hashtag_adjacent_to_punctuation(spark):
    """'#election,' and '#election!' are the same tag."""
    assert hashtags_of(spark, "vote #election, now #results!") == ["election", "results"]


def test_no_hashtags_gives_empty_array_not_null(spark):
    assert hashtags_of(spark, "nothing tagged here") == []


def test_empty_text_gives_empty_array(spark):
    assert hashtags_of(spark, "") == []


def test_url_fragment_is_not_mistaken_for_a_hashtag(spark):
    """Extraction runs on cleaned text, so a stripped URL cannot contribute."""
    assert hashtags_of(spark, "read http://example.com/page#section") == []


def test_hash_alone_is_not_a_hashtag(spark):
    assert hashtags_of(spark, "the # symbol") == []


# --- retweets ----------------------------------------------------------------


def test_retweet_filter(spark):
    """C24. Matched on RAW text, because cleaning lowercases the marker away."""
    df = frame(
        spark,
        [
            ("RT @someone: original message", "a"),
            ("a genuine post", "b"),
            ("RT this is also a retweet", "c"),
        ],
    )
    remaining = [row["text"] for row in preprocess.filter_retweets(df).collect()]
    assert remaining == ["a genuine post"]


def test_rt_inside_text_is_not_a_retweet(spark):
    """Only a leading 'RT ' marks a retweet; 'art rt' must survive."""
    df = frame(spark, [("i support rt journalism", "a"), ("start rt now", "b")])
    assert preprocess.filter_retweets(df).count() == 2


def test_leading_whitespace_before_rt_still_counts_as_a_retweet(spark):
    df = frame(spark, [("  RT @a: copied", "x")])
    assert preprocess.filter_retweets(df).count() == 0


def test_lowercase_rt_is_not_filtered(spark):
    """The corpus marker is uppercase; lowercasing this rule would delete
    every tweet starting with the word 'rt'."""
    df = frame(spark, [("rt if you agree", "x")])
    assert preprocess.filter_retweets(df).count() == 1


# --- dedup key ---------------------------------------------------------------


def dedup_of(spark, text, user="someone"):
    df = preprocess.clean_text(frame(spark, [(text, user)]))
    return preprocess.add_dedup_key(df).first()[preprocess.DEDUP_KEY]


def test_dedup_key_is_stable_across_calls(spark):
    """C18 and idempotent writes both depend on this being deterministic."""
    assert dedup_of(spark, "same text") == dedup_of(spark, "same text")


def test_dedup_key_differs_for_different_text(spark):
    assert dedup_of(spark, "one thing") != dedup_of(spark, "another thing")


def test_dedup_key_differs_for_different_user(spark):
    """Two people posting identical text are two records, not a duplicate."""
    assert dedup_of(spark, "same text", "alice") != dedup_of(spark, "same text", "bob")


def test_dedup_key_ignores_cosmetic_differences(spark):
    """Case and URL noise must not produce two keys for one logical record."""
    assert dedup_of(spark, "Hello World") == dedup_of(spark, "hello    world")


def test_dedup_key_is_a_sha1_hex_digest(spark):
    key = dedup_of(spark, "anything")
    assert len(key) == 40
    assert all(character in "0123456789abcdef" for character in key)


# --- the full chain ----------------------------------------------------------


def test_prepare_applies_every_transform(spark):
    df = frame(spark, [("RT @a: skip me", "x"), ("Great #Day http://t.co/z", "y")])
    result = preprocess.prepare(df, emoji_strategy="strip").collect()

    assert len(result) == 1, "the retweet should have been filtered"
    row = result[0]
    assert row[preprocess.TEXT_CLEAN] == "great #day"
    assert row[preprocess.HASHTAGS] == ["day"]
    assert len(row[preprocess.DEDUP_KEY]) == 40
    assert row["text"] == "Great #Day http://t.co/z", "raw text must be preserved"


def test_prepare_can_keep_retweets_for_measurement(spark):
    df = frame(spark, [("RT @a: copied", "x")])
    assert preprocess.prepare(df, drop_retweets=False).count() == 1
