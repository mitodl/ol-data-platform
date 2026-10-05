# ruff: noqa: E501
"""Tests for the YouTube webhook delivery payload construction.

These cover the pure half of ``assets/youtube.py``: the payload has to match
``transform_playlist`` / ``transform_video`` in mit-learn's
``learning_resources/etl/youtube.py`` key for key, because ``load_playlist`` and
``load_video`` pop a fixed set of keys and pass the rest to ``LearningResource``
as model fields.
"""

import json

import pytest
from delivery.assets.youtube import (
    MIN_PLAYLIST_VIDEOS,
    MIN_PLAYLISTS,
    assert_deliverable,
    batch_resources,
    build_playlist_resources,
    clean_youtube_description,
)

# transform_video's keys.
VIDEO_KEYS = {
    "readable_id",
    "platform",
    "etl_source",
    "resource_type",
    "title",
    "description",
    "image",
    "last_modified",
    "url",
    "offered_by",
    "published",
    "video",
    "availability",
    "youtube_id",
}
# transform_playlist's keys, plus the three the webhook adds.
PLAYLIST_KEYS = {
    "playlist_id",
    "title",
    "published",
    "platform",
    "etl_source",
    "offered_by",
    "videos",
    "url",
    "image",
    "availability",
    "create_videos",
    "readable_id",
    "resource_type",
    "channel",
}

# The cases of test_clean_youtube_description in mit-learn's
# learning_resources/etl/youtube_test.py, copied as they are.
CLEAN_DESCRIPTION_CASES = [
    (
        "Nancy L. Rose and Janis Melvold share how two different approaches—Plickers and workshops—engage students in conversation and challenge them to deepen their knowledge. \n\nNancy L. Rose is Charles P. Kindleberger Professor of Applied Economics and MacVicar Faculty Fellow at MIT, and a Visiting Scholar at Harvard Kennedy School.\n\nJanis Melvold is a linguist and Lecturer II in Comparative Media Studies/Writing at MIT. \n\nChapters:\n0:00 INTRODUCTION\n\n1:29 NANCY L. ROSE &amp; USE OF PLICKERS\n2:02 Deciding to use Plickers\n2:36 About 14.20/14.200 Industrial Organization\n3:52 Plickers for participation and in-class feedback\n4:11 What is a Plicker?\n6:27 Generating discussion\n8:19 Lowering the barrier to participation\n9:40 Assessing translational skills\n12:00 Real-time feedback on lectures\n13:47 Checking student understanding of lecture\n15:43 Illustrating progress in learning\n\n17:48 JANIS MELVOLD &amp; WRITING WORKSHOPS\n18:17 About writing workshops\n18:37 MIT's communication requirement\n20:29 Intended outcomes of CI-H subjects\n21:12 Toolkit for embedded writing instruction\n23:04 About 24.900 Intro to Linguistics\n26:08 Overview of 24.900 writing workshops\n26:54 Instruction &amp; active engagement\n27:35 Leveraging sample writing from past students\n28:24 Example of workshop exercise: Critical summary\n30:41 Example of workshop exercise: Research findings\n34:32 Advantages &amp; limitations of writing workshops\n36:37 Considerations for the future\n\n38:32 DISCUSSION &amp; Q/A\n39:39 Student feedback\n43:12 Course iteration\n47:40 Plickers &amp; student explanations\n49:10 Optional attendance: Writing workshops\n\nMIT Open Learning:\nhttps://openlearning.mit.edu/residential-education\nhttps://openlearning.mit.edu/events/uncovering-student-perspectives-course-concepts-tips-economist-and-linguist",
        "Nancy L. Rose and Janis Melvold share how two different approaches—Plickers and workshops—engage students in conversation and challenge them to deepen their knowledge. \n\nNancy L. Rose is Charles P. Kindleberger Professor of Applied Economics and MacVicar Faculty Fellow at MIT, and a Visiting Scholar at Harvard Kennedy School.\n\nJanis Melvold is a linguist and Lecturer II in Comparative Media Studies/Writing at MIT.",
    ),
    (
        "MIT 9.35, Spring 2024\nInstructor: Josh McDermott\nView the complete course: https://ocw.mit.edu/courses/9-35-perception-spring-2024\nYouTube Playlist: https://www.youtube.com/playlist?list=PLUl4u3cNGP62-9RweyYBIpkqfo5dfcuS8\n\nThis lecture discusses the senses of smell and taste, with emphasis on the working of the sense receptors in the nose and tongue.\n\nLicense: Creative Commons BY-NC-SA\nMore information at https://ocw.mit.edu/terms\nMore courses at https://ocw.mit.edu\nSupport OCW at http://ow.ly/a1If50zVRl\n\nWe encourage constructive comments and discussion on OCW's YouTube and other social media channels. Personal attacks, hate speech, trolling, and inappropriate comments are not allowed and may be removed. More details at https://ocw.mit.edu/comments.",
        "This lecture discusses the senses of smell and taste, with emphasis on the working of the sense receptors in the nose and tongue.",
    ),
    (
        "MIT 21H.151 Dynastic China, Fall 2024 \nInstructor: Tristan G. Brown \nView the complete course: https://ocw.mit.edu/courses/21h-151-dynastic-china-fall-2024\nYouTube Playlist: https://www.youtube.com/playlist?list=PLUl4u3cNGP60g8vnEsLGuA4Kt-d5vNqy9\n\nProf. Brown discusses the emergence of Taiwan during the Qing Dynasty.\n\nLicense: Creative Commons BY-NC-SA\nMore information at https://ocw.mit.edu/terms\nMore courses at https://ocw.mit.edu\nSupport OCW at http://ow.ly/a1If50zVRlQ\n\nWe encourage constructive comments and discussion on OCW's YouTube and other social media channels. Personal attacks, hate speech, trolling, and inappropriate comments are not allowed and may be removed. More details at https://ocw.mit.edu/comments.",
        "Prof. Brown discusses the emergence of Taiwan during the Qing Dynasty.",
    ),
    (
        "What can we do at MIT to prepare our students to solve the challenges of their time? Every learner at MIT can impact the future of humankind and the planet—calling on us to embrace new approaches to an MIT education. In this video, panelists share their perspectives on deeply engaging students and the future of teaching and learning at MIT. \n\nSpeakers:\nOpening Remarks by Daniel E. Hastings, Interim Vice Chancellor, and the Cecil and Ida Green Education Professor of Aeronautics and Astronautics\nChris Capozzola, Elting E. Morison Professor of History &amp; Senior Associate Dean for Open Learning\nAdam Martin, Professor of Biology &amp; Co-Chair of the Task Force on the Undergraduate Academic Program\nAmitava 'Babi' Mitra, Founding Executive Director, New Engineering Education Transformation (NEET), School of Engineering\nSusan Silbey, Leon and Anne Goldberg Professor of Humanities, Sociology and Anthropology; &amp; Professor of Behavioral and Policy Sciences at Sloan School of Management\nModerator: Janet Rankin, Director of MIT Teaching + Learning Lab\n\nChapters:\n0:00 Opening remarks\n6:11 Panelist introductions\n8:35 Mens et manus (mind and hand): Strengths &amp; challenges\n16:25 Important changes or improvements to MIT education\n26:06 Examples of deeply engaging students\n39:45 Transforming mens et manus for the future\n47:53 Technology, society, and culture\n48:23 Helping students grasp the purpose and relevance of courses\n53:00 Expanding the idea of campus\n56:10 Supporting students with different goals\n1:00:57 Scaling the MIT experience for the world\n1:04:09 Envisioning a campus that values communities\n\nFestival: https://openlearning.mit.edu/mit-faculty/festival-learning \nMIT Residential Education: https://openlearning.mit.edu/residential-education",
        "What can we do at MIT to prepare our students to solve the challenges of their time? Every learner at MIT can impact the future of humankind and the planet—calling on us to embrace new approaches to an MIT education. In this video, panelists share their perspectives on deeply engaging students and the future of teaching and learning at MIT. \n\nOpening Remarks by Daniel E. Hastings, Interim Vice Chancellor, and the Cecil and Ida Green Education Professor of Aeronautics and Astronautics\nChris Capozzola, Elting E. Morison Professor of History &amp; Senior Associate Dean for Open Learning\nAdam Martin, Professor of Biology &amp; Co-Chair of the Task Force on the Undergraduate Academic Program\nAmitava 'Babi' Mitra, Founding Executive Director, New Engineering Education Transformation (NEET), School of Engineering\nSusan Silbey, Leon and Anne Goldberg Professor of Humanities, Sociology and Anthropology; &amp; Professor of Behavioral and Policy Sciences at Sloan School of Management\nModerator: Janet Rankin, Director of MIT Teaching + Learning Lab",
    ),
]


def _channel(channel_id="UC-mitx"):
    return {
        "channel_id": channel_id,
        "title": "MITx Videos",
        "offered_by": "mitx",
        "etl_source": "youtube",
        "published": True,
        "dlt_load_id": "1759300000.1",
    }


def _playlist(readable_id="PL-one", channel_id="UC-mitx", **overrides):
    return {
        "readable_id": readable_id,
        "channel_id": channel_id,
        "title": "A playlist",
        "url": f"https://www.youtube.com/playlist?list={readable_id}",
        "image_url": "https://i.ytimg.com/vi/abc/hqdefault.jpg",
        "image_alt": "A playlist",
        "offered_by": "mitx",
        "create_videos": True,
        "etl_source": "youtube",
        "platform": "youtube",
        "resource_type": "video_playlist",
        "availability": "anytime",
        "published": True,
        "dlt_load_id": "1759300000.1",
        **overrides,
    }


def _video(readable_id="vid-1", **overrides):
    return {
        "readable_id": readable_id,
        "youtube_id": readable_id,
        "title": f"Video {readable_id}",
        "description_raw": "About the video.\nLicense: Creative Commons BY-NC-SA",
        "url": f"https://www.youtube.com/watch?v={readable_id}",
        "image_url": f"https://i.ytimg.com/vi/{readable_id}/hqdefault.jpg",
        "last_modified": "2024-05-01T12:00:00Z",
        "duration": "PT1H2M3S",
        "etl_source": "youtube",
        "platform": "youtube",
        "resource_type": "video",
        "availability": "anytime",
        "published": True,
        **overrides,
    }


def _membership(playlist, video, position):
    return {
        "playlist_readable_id": playlist,
        "video_readable_id": video,
        "position": position,
    }


@pytest.mark.parametrize(("original", "cleaned"), CLEAN_DESCRIPTION_CASES)
def test_clean_youtube_description_matches_mit_learn(original, cleaned):
    assert clean_youtube_description(original) == cleaned


@pytest.mark.parametrize("empty", [None, ""])
def test_clean_youtube_description_of_nothing_is_empty_string(empty):
    """clean_data returns "" for a falsy value, so transform_video sends ""."""
    assert clean_youtube_description(empty) == ""


def test_clean_youtube_description_strips_html_before_dropping_lines():
    """transform_video runs clean_data first, so a link's text survives nh3 and
    the line goes only if it still carries a URL.
    """
    raw = 'Read <a href="https://example.com">the notes</a>\n<script>x()</script>Intro'
    assert clean_youtube_description(raw) == "Read the notes\nIntro"


def test_playlist_resource_has_exactly_the_expected_keys():
    (resource,) = build_playlist_resources(
        [_channel()],
        [_playlist()],
        [_membership("PL-one", "vid-1", 0)],
        [_video()],
    )

    assert set(resource) == PLAYLIST_KEYS
    assert resource["readable_id"] == resource["playlist_id"] == "PL-one"
    assert resource["resource_type"] == "video_playlist"
    assert resource["channel"] == {
        "channel_id": "UC-mitx",
        "title": "MITx Videos",
        "published": True,
    }
    assert resource["offered_by"] == {"code": "mitx"}
    assert resource["image"] == {
        "url": "https://i.ytimg.com/vi/abc/hqdefault.jpg",
        "alt": "A playlist",
    }
    assert resource["create_videos"] is True

    (video,) = resource["videos"]
    assert set(video) == VIDEO_KEYS
    assert video["description"] == "About the video."
    assert video["image"] == {"url": "https://i.ytimg.com/vi/vid-1/hqdefault.jpg"}
    assert video["video"] == {"duration": "PT1H2M3S"}
    assert video["offered_by"] == {"code": "mitx"}
    assert video["youtube_id"] == "vid-1"


def test_videos_are_nested_in_playlist_order():
    (resource,) = build_playlist_resources(
        [_channel()],
        [_playlist()],
        [
            _membership("PL-one", "vid-c", 2),
            _membership("PL-one", "vid-a", 0),
            _membership("PL-one", "vid-b", 1),
        ],
        [_video("vid-a"), _video("vid-b"), _video("vid-c")],
    )

    assert [video["readable_id"] for video in resource["videos"]] == [
        "vid-a",
        "vid-b",
        "vid-c",
    ]


def test_video_in_two_playlists_takes_each_playlists_offered_by():
    resources = build_playlist_resources(
        [_channel(), _channel("UC-ocw")],
        [
            _playlist("PL-one"),
            _playlist("PL-two", "UC-ocw", offered_by="ocw", create_videos=False),
        ],
        [_membership("PL-one", "vid-1", 0), _membership("PL-two", "vid-1", 0)],
        [_video()],
    )

    by_id = {resource["readable_id"]: resource for resource in resources}
    assert by_id["PL-one"]["videos"][0]["offered_by"] == {"code": "mitx"}
    assert by_id["PL-two"]["videos"][0]["offered_by"] == {"code": "ocw"}
    assert by_id["PL-two"]["create_videos"] is False
    assert by_id["PL-two"]["channel"]["channel_id"] == "UC-ocw"


def test_playlist_with_no_videos_and_no_offered_by():
    """A create_videos playlist is published with no videos, as on mit-learn main."""
    (resource,) = build_playlist_resources(
        [_channel()], [_playlist(offered_by=None)], [], []
    )

    assert resource["videos"] == []
    assert resource["offered_by"] is None


def test_assert_deliverable_accepts_a_real_read():
    assert_deliverable(MIN_PLAYLISTS, MIN_PLAYLIST_VIDEOS)


def test_assert_deliverable_refuses_too_few_playlists():
    with pytest.raises(RuntimeError, match="YouTube playlists"):
        assert_deliverable(MIN_PLAYLISTS - 1, MIN_PLAYLIST_VIDEOS)


def test_assert_deliverable_refuses_playlists_emptied_of_videos():
    """Healthy playlists over an empty membership or videos table."""
    with pytest.raises(RuntimeError, match="unpublishes the videos"):
        assert_deliverable(MIN_PLAYLISTS * 10, 0)


def _body_bytes(batch):
    return len(json.dumps({"resources": batch}, separators=(",", ":")).encode())


def test_batches_keep_every_resource_in_order_under_the_limit():
    resources = [
        {"readable_id": f"PL-{number}", "videos": [], "title": "é" * 40}
        for number in range(25)
    ]
    limit = _body_bytes(resources[:4])

    batches = list(batch_resources(resources, max_bytes=limit))

    assert [resource for batch in batches for resource in batch] == resources
    assert all(_body_bytes(batch) <= limit for batch in batches)
    assert len(batches[0]) == 4


def test_batching_nothing_sends_nothing():
    assert list(batch_resources([])) == []


def test_a_resource_over_the_limit_alone_is_an_error():
    resource = {"readable_id": "PL-huge", "videos": [], "title": "x" * 500}

    with pytest.raises(RuntimeError, match="PL-huge"):
        list(batch_resources([resource], max_bytes=200))
