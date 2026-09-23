"""A small bundled emoji sentiment lexicon for EMOJI_STRATEGY='map'.

Deliberately small and hand-checked rather than large and noisy. Emoji not
listed here fall through to the neutral token, so coverage gaps degrade to
"neutral" instead of silently vanishing.

The three replacement tokens survive tokenisation as single tokens, which is
the point -- they become features the classifier can actually learn from.
"""

from __future__ import annotations

POSITIVE_TOKEN = "__emoji_pos__"
NEGATIVE_TOKEN = "__emoji_neg__"
NEUTRAL_TOKEN = "__emoji_neu__"

POSITIVE = [
    "\U0001F600", "\U0001F601", "\U0001F602", "\U0001F603", "\U0001F604",
    "\U0001F605", "\U0001F606", "\U0001F609", "\U0001F60A", "\U0001F60B",
    "\U0001F60D", "\U0001F60E", "\U0001F60F", "\U0001F617", "\U0001F618",
    "\U0001F619", "\U0001F61A", "\U0001F61C", "\U0001F61D", "\U0001F642",
    "\U0001F643", "\U0001F929", "\U0001F970", "\U0001F973", "\U0001F929",
    "\U0001F923", "\U0001F44D", "\U0001F44F", "\U0001F64C", "\U0001F64F",
    "\U0001F389", "\U0001F38A", "\U0001F496", "\U0001F497", "\U0001F49B",
    "\U0001F49C", "\U0001F49A", "\U0001F499", "\U00002764", "\U0001F495",
    "\U00002728", "\U0001F525", "\U0001F4AF", "\U00002665", "\U0000263A",
]

NEGATIVE = [
    "\U0001F612", "\U0001F613", "\U0001F614", "\U0001F615", "\U0001F616",
    "\U0001F61E", "\U0001F61F", "\U0001F620", "\U0001F621", "\U0001F622",
    "\U0001F623", "\U0001F624", "\U0001F625", "\U0001F626", "\U0001F627",
    "\U0001F628", "\U0001F629", "\U0001F62A", "\U0001F62B", "\U0001F62C",
    "\U0001F62D", "\U0001F62E", "\U0001F62F", "\U0001F630", "\U0001F631",
    "\U0001F632", "\U0001F633", "\U0001F635", "\U0001F636", "\U0001F637",
    "\U0001F641", "\U0001F644", "\U0001F92C", "\U0001F92E", "\U0001F974",
    "\U0001F44E", "\U0001F494", "\U00002639", "\U0001F621", "\U0001F62B",
]

#: Broad ranges used to catch anything not explicitly classified above.
#: Java regex syntax -- Spark's regexp_replace compiles with java.util.regex.
EMOJI_RANGES = (
    r"[\x{1F300}-\x{1FAFF}\x{1F900}-\x{1F9FF}\x{2600}-\x{27BF}"
    r"\x{1F1E6}-\x{1F1FF}\x{2B00}-\x{2BFF}\x{FE0F}\x{20E3}]"
)


def _alternation(emoji: list[str]) -> str:
    """Regex alternation over literal emoji, longest first.

    Alternation rather than a character class: supplementary-plane characters
    inside a class are a recurring source of subtle JVM-vs-Python regex
    differences, and this codebase cannot afford the batch and stream paths
    disagreeing.
    """
    unique = sorted(set(emoji), key=len, reverse=True)
    return "|".join(unique)


POSITIVE_RE = _alternation(POSITIVE)
NEGATIVE_RE = _alternation(NEGATIVE)
