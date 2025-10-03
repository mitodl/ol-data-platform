"""Helper class for custom sql functions for use with pypika queries."""

from pypika import CustomFunction

# Presto SQL functions
SplitPart = CustomFunction("SPLIT_PART", ["string", "delimiter", "part"])

Position = CustomFunction("POSITION", ["input"])

MinBy = CustomFunction("MIN_BY", ["value1", "value2"])


# Postgres SQL functions
class Excluded:
    def __init__(self, field):
        self.field = field

    def __str__(self):
        return f"EXCLUDED.{self.field}"
