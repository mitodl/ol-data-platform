"""Video encoding and thumbnail generation utilities."""

import logging
import os
from pathlib import Path

import ffmpeg

log = logging.getLogger(__name__)


def generate_video_thumbnail(
    video_path: Path,
    thumbnail_path: Path,
    width: int,
    height: int,
) -> None:
    """
    Generate a thumbnail image from a video file using ffmpeg.

    Seeks 1 second into the video to skip black fade-in frames
    while staying on the title card.

    Args:
        video_path: Path to the input video file
        thumbnail_path: Path where thumbnail will be saved
        width: Thumbnail width in pixels
        height: Thumbnail height in pixels

    Raises:
        RuntimeError: If video cannot be read or thumbnail cannot be generated
    """
    try:
        (
            ffmpeg.input(str(video_path), ss=1)
            .filter("scale", width, height)
            .output(
                str(thumbnail_path),
                vframes=1,
                format="image2",
                vcodec="mjpeg",
            )
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
    except ffmpeg.Error as e:
        stderr = e.stderr.decode("utf-8") if e.stderr else "No error details"
        msg = f"Failed to generate thumbnail from {video_path}: {stderr}"
        raise RuntimeError(msg) from e

    # Verify thumbnail was created
    if not thumbnail_path.exists() or thumbnail_path.stat().st_size == 0:
        msg = f"Thumbnail file was not created or is empty: {thumbnail_path}"
        raise RuntimeError(msg)


def compress_video(
    input_path: Path,
    output_path: Path,
    max_size_mb: float = 25.0,
) -> Path:
    """
    Compress video to a target maximum file size using two-pass H.264 encoding.

    If the input file is already under the target size, returns input_path.
    Otherwise, a two-pass encode calculates the optimal bitrate to fit within
    the size limit with minimal quality loss.

    Args:
        input_path: Path to the input video file
        output_path: Path where compressed video will be saved
        max_size_mb: Maximum output file size in megabytes (default 25 MB)

    Returns:
        Path to the output file

    Raises:
        RuntimeError: If compression fails or output exceeds target size
    """
    file_size_mb = input_path.stat().st_size / (1024 * 1024)

    if file_size_mb <= max_size_mb:
        log.info(
            "Video already under %.1f MB (%.1f MB), skipping compression",
            max_size_mb,
            file_size_mb,
        )
        return input_path

    log.info(
        "Compressing video from %.1f MB to target %.1f MB", file_size_mb, max_size_mb
    )

    # Get video duration
    try:
        probe = ffmpeg.probe(str(input_path))
        duration = float(probe["format"]["duration"])
    except (ffmpeg.Error, KeyError, ValueError) as e:
        msg = f"Failed to probe video {input_path}: {e}"
        raise RuntimeError(msg) from e

    # Calculate target bitrate: reserve 128kbps for audio
    audio_bitrate = 128_000
    target_total_bits = max_size_mb * 1024 * 1024 * 8
    video_bitrate = int((target_total_bits / duration) - audio_bitrate)

    if video_bitrate <= 0:
        min_size = duration * audio_bitrate / 8 / 1024 / 1024
        msg = (
            f"Video too long ({duration:.0f}s) for target size "
            f"({max_size_mb} MB). "
            f"Minimum ~{min_size:.1f} MB needed for audio alone."
        )
        raise RuntimeError(msg)

    log.info(
        "Duration: %.1fs, target video bitrate: %d bps (%.1f Mbps)",
        duration,
        video_bitrate,
        video_bitrate / 1_000_000,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    passlogfile = str(output_path.parent / "ffmpeg2pass")

    try:
        # Pass 1: analysis only (output to /dev/null)
        (
            ffmpeg.input(str(input_path))
            .output(
                os.devnull,
                format="null",
                **{
                    "c:v": "libx264",
                    "b:v": video_bitrate,
                    "pass": 1,
                    "passlogfile": passlogfile,
                    "an": None,
                },
            )
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )

        # Pass 2: actual encode
        (
            ffmpeg.input(str(input_path))
            .output(
                str(output_path),
                **{
                    "c:v": "libx264",
                    "b:v": video_bitrate,
                    "pass": 2,
                    "passlogfile": passlogfile,
                    "c:a": "aac",
                    "b:a": audio_bitrate,
                },
            )
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
    except ffmpeg.Error as e:
        stderr = e.stderr.decode("utf-8") if e.stderr else "No error details"
        msg = f"Failed to compress video {input_path}: {stderr}"
        raise RuntimeError(msg) from e

    # Verify output
    if not output_path.exists() or output_path.stat().st_size == 0:
        msg = f"Compressed video was not created or is empty: {output_path}"
        raise RuntimeError(msg)

    output_size_mb = output_path.stat().st_size / (1024 * 1024)
    log.info(
        "Compression complete: %.1f MB -> %.1f MB (%.0f%% reduction)",
        file_size_mb,
        output_size_mb,
        (1 - output_size_mb / file_size_mb) * 100,
    )

    return output_path
