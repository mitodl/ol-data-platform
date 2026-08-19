"""Tests for the podcast webhook delivery payload construction.

These cover the pure transform half of ``assets/podcasts.py`` -- the part that
has to match ``learning_resources/etl/podcast.py`` in mit-learn key for key,
because MIT Learn's ``load_podcast``/``load_podcast_episode`` pop a fixed set
of keys and pass the remainder straight to ``LearningResource`` as model
fields. An extra key there is an error, not an ignored extra, so the shape
assertions below are exact.
"""

import pytest
from learning_resources.assets.podcasts import (
    _iso8601_duration,
    _parse_pub_date,
    _topics,
    build_podcast_resources,
)

# Keys mit-learn's load_podcast() pops, plus the LearningResource fields it
# forwards. Anything outside this set would blow up update_or_create().
PODCAST_KEYS = {
    "readable_id",
    "title",
    "etl_source",
    "resource_type",
    "offered_by",
    "description",
    "image",
    "published",
    "url",
    "topics",
    "episodes",
    "podcast",
    "availability",
}
EPISODE_KEYS = {
    "readable_id",
    "etl_source",
    "resource_type",
    "title",
    "offered_by",
    "description",
    "url",
    "image",
    "last_modified",
    "published",
    "topics",
    "podcast_episode",
    "availability",
}


@pytest.fixture
def podcast_row():
    return {
        "readable_id": "feeds.example.com/mit-podcast/",
        "title": "MIT Podcast",
        "description": "A podcast about MIT.",
        "url": "https://example.com/mit-podcast",
        "image_url": "https://example.com/cover.png",
        "topics": "Science, Engineering",
        "offered_by": "MIT Open Learning",
        "rss_url": "https://feeds.example.com/mit-podcast/",
        "apple_podcasts_url": "https://podcasts.apple.com/mit",
        "google_podcasts_url": None,
    }


@pytest.fixture
def episode_row():
    return {
        "readable_id": "episode-guid-1",
        "podcast_readable_id": "feeds.example.com/mit-podcast/",
        "title": "Episode 1",
        "description": "The first episode.",
        "url": "https://example.com/ep1",
        "audio_url": "https://example.com/ep1.mp3",
        "episode_link": "https://example.com/ep1",
        "image_url": None,
        "duration_raw": "1:02:03",
        "published_on_raw": "Wed, 02 Oct 2002 13:00:00 GMT",
    }


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1:02:03", "PT1H2M3S"),
        ("02:03", "PT2M3S"),
        ("3600", "PT1H"),
        ("0", "PT0S"),
        ("0:00:00", "PT0S"),
        ("PT1H30M", "PT1H30M"),
        ("", None),
        (None, None),
        ("not a duration", None),
    ],
)
def test_iso8601_duration(raw, expected):
    """Durations normalize the way mit-learn's iso8601_duration does."""
    assert _iso8601_duration(raw) == expected


def test_iso8601_duration_fits_the_column_under_ten_hours():
    """Durations up to 9h59m59s fit PodcastEpisode.duration (a 10-char column)."""
    max_duration_length = 10
    assert len(_iso8601_duration("9:59:59")) == max_duration_length


def test_iso8601_duration_overflows_the_column_past_ten_hours():
    """A >=10h episode with non-zero minutes AND seconds overflows the column.

    Zero components are omitted, so "10:00:00" stays short ("PT10H"); it takes
    all three parts being non-zero to reach 11 characters.

    This is NOT a divergence introduced here -- mit-learn's own
    iso8601_duration() emits the identical string, so the Celery ETL has the
    same latent failure. It is asserted rather than worked around so the two
    paths stay byte-identical during parallel validation; deviating would make
    a cutover diff that is noise rather than signal.
    """
    max_duration_length = 10
    assert _iso8601_duration("10:00:00") == "PT10H"
    assert len(_iso8601_duration("10:30:15")) > max_duration_length


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Wed, 02 Oct 2002 13:00:00 GMT", "2002-10-02T13:00:00+00:00"),
        ("", None),
        (None, None),
        ("yesterday", None),
    ],
)
def test_parse_pub_date(raw, expected):
    assert _parse_pub_date(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Science, Engineering", [{"name": "Science"}, {"name": "Engineering"}]),
        ("Science", [{"name": "Science"}]),
        ("Science, , Engineering", [{"name": "Science"}, {"name": "Engineering"}]),
        ("", []),
        (None, []),
    ],
)
def test_topics(raw, expected):
    assert _topics(raw) == expected


def test_podcast_payload_shape(podcast_row, episode_row):
    """The podcast payload carries exactly the keys load_podcast() expects."""
    [podcast] = build_podcast_resources([podcast_row], [episode_row])

    assert set(podcast) == PODCAST_KEYS
    assert podcast["readable_id"] == "feeds.example.com/mit-podcast/"
    assert podcast["etl_source"] == "podcast"
    assert podcast["resource_type"] == "podcast"
    assert podcast["availability"] == "anytime"
    assert podcast["published"] is True
    assert podcast["offered_by"] == {"name": "MIT Open Learning"}
    assert podcast["image"] == {"url": "https://example.com/cover.png"}
    assert podcast["topics"] == [{"name": "Science"}, {"name": "Engineering"}]
    assert podcast["podcast"] == {
        "apple_podcasts_url": "https://podcasts.apple.com/mit",
        "google_podcasts_url": None,
        "rss_url": "https://feeds.example.com/mit-podcast/",
    }


def test_episode_payload_shape(podcast_row, episode_row):
    """The episode payload carries exactly the keys load_podcast_episode() expects."""
    [podcast] = build_podcast_resources([podcast_row], [episode_row])
    [episode] = podcast["episodes"]

    assert set(episode) == EPISODE_KEYS
    assert episode["readable_id"] == "episode-guid-1"
    assert episode["resource_type"] == "podcast_episode"
    assert episode["last_modified"] == "2002-10-02T13:00:00+00:00"
    assert episode["podcast_episode"] == {
        "audio_url": "https://example.com/ep1.mp3",
        "episode_link": "https://example.com/ep1",
        "duration": "PT1H2M3S",
    }


def test_episode_omits_rss(podcast_row, episode_row):
    """`rss` is left out so update_or_create does not blank an existing value."""
    [podcast] = build_podcast_resources([podcast_row], [episode_row])
    assert "rss" not in podcast["episodes"][0]["podcast_episode"]


def test_episode_inherits_channel_topics_and_offered_by(podcast_row, episode_row):
    """Episodes inherit topics/offered_by from their channel, as transform_episode."""
    [podcast] = build_podcast_resources([podcast_row], [episode_row])
    [episode] = podcast["episodes"]

    assert episode["topics"] == podcast["topics"]
    assert episode["offered_by"] == podcast["offered_by"]


def test_episode_falls_back_to_channel_image(podcast_row, episode_row):
    """An episode with no image of its own uses the channel's cover art."""
    [podcast] = build_podcast_resources([podcast_row], [episode_row])
    assert podcast["episodes"][0]["image"] == {"url": "https://example.com/cover.png"}


def test_episode_prefers_its_own_image(podcast_row, episode_row):
    episode_row["image_url"] = "https://example.com/ep1.png"
    [podcast] = build_podcast_resources([podcast_row], [episode_row])
    assert podcast["episodes"][0]["image"] == {"url": "https://example.com/ep1.png"}


def test_episodes_group_by_podcast(podcast_row, episode_row):
    """Episodes attach to their own podcast and nowhere else."""
    other_podcast = {**podcast_row, "readable_id": "feeds.example.com/other/"}
    other_episode = {
        **episode_row,
        "readable_id": "episode-guid-2",
        "podcast_readable_id": "feeds.example.com/other/",
    }

    resources = build_podcast_resources(
        [podcast_row, other_podcast], [episode_row, other_episode]
    )

    by_id = {resource["readable_id"]: resource for resource in resources}
    assert [
        episode["readable_id"]
        for episode in by_id["feeds.example.com/mit-podcast/"]["episodes"]
    ] == ["episode-guid-1"]
    assert [
        episode["readable_id"]
        for episode in by_id["feeds.example.com/other/"]["episodes"]
    ] == ["episode-guid-2"]


def test_podcast_with_no_episodes(podcast_row):
    """A podcast whose feed yielded no usable items still delivers, with no episodes."""
    [podcast] = build_podcast_resources([podcast_row], [])
    assert podcast["episodes"] == []


def test_orphan_episode_is_dropped(podcast_row, episode_row):
    """An episode whose podcast is absent is not smuggled into another one."""
    orphan = {**episode_row, "podcast_readable_id": "feeds.example.com/gone/"}
    [podcast] = build_podcast_resources([podcast_row], [orphan])
    assert podcast["episodes"] == []


def test_missing_offered_by_is_none(podcast_row, episode_row):
    """offered_by is None rather than {"name": None} when the config omits it."""
    podcast_row["offered_by"] = None
    [podcast] = build_podcast_resources([podcast_row], [episode_row])

    assert podcast["offered_by"] is None
    assert podcast["episodes"][0]["offered_by"] is None


def test_missing_image_is_none(podcast_row, episode_row):
    podcast_row["image_url"] = None
    [podcast] = build_podcast_resources([podcast_row], [episode_row])

    assert podcast["image"] is None
    assert podcast["episodes"][0]["image"] is None
