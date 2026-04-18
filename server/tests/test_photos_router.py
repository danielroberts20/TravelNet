"""
test_photos_router.py — Unit tests for upload/photos/router.py pure helpers.

Covers _parse_taken_at():
  - EXIF DateTimeOriginal + OffsetTimeOriginal → UTC ISO string
  - EXIF DateTimeDigitized used when DateTimeOriginal absent
  - OffsetTime used when OffsetTimeOriginal absent
  - Malformed EXIF datetime skipped, falls back to date_created
  - No EXIF, date_created with Z suffix → UTC ISO string
  - No EXIF, date_created with tz offset → converted to UTC
  - No EXIF, no date_created → taken_at used
  - All timestamp sources missing → None
  - Empty exif dict → falls through to date_created

Covers _resolve_source_app():
  - Album name match (case-insensitive) → app name
  - Album name partial match ("Instagram Saves" → "instagram")
  - Camera make present, no album → "camera"
  - No album, no camera_make, no exif → "unknown"
  - UserComment b64-encoded app name → identified
  - Malformed UserComment b64 → "unknown"

Covers _normalise_media_type():
  - is_screen_recording=True → "screen_recording" (highest priority)
  - is_screenshot=True → "screenshot"
  - duration_s set → "video"
  - No flags, no duration → "photo"
  - screen_recording beats screenshot when both True

Covers _insert_batch():
  - Item without file_path → skipped (not inserted)
  - Item without resolvable timestamp → not inserted
  - GPS 0.0/0.0 treated as missing (Shortcuts null-sentinel)
  - Valid GPS → get_place_id called with correct args
  - No GPS → get_place_id not called, gps_source=None
  - Successful insert → photos_table.insert called with correct record
  - Duplicate file_path (insert returns False) → not counted as inserted
  - media_type and source_app are derived and stored on the record
"""

import pytest
from base64 import b64encode
from unittest.mock import patch, MagicMock

from upload.photos.router import (
    PhotoItem,
    _parse_taken_at,
    _resolve_source_app,
    _normalise_media_type,
    _insert_batch,
)


def _item(**kwargs) -> PhotoItem:
    return PhotoItem(**kwargs)


# ---------------------------------------------------------------------------
# _parse_taken_at
# ---------------------------------------------------------------------------

class TestParseTakenAt:

    def test_exif_datetime_original_with_offset(self):
        # 10:30 at +05:30 → 05:00 UTC
        item = _item(exif={
            "DateTimeOriginal": "2024:06:15 10:30:00",
            "OffsetTimeOriginal": "+05:30",
        })
        assert _parse_taken_at(item) == "2024-06-15T05:00:00Z"

    def test_exif_datetime_original_utc_offset(self):
        item = _item(exif={
            "DateTimeOriginal": "2024:06:15 10:30:00",
            "OffsetTimeOriginal": "+00:00",
        })
        assert _parse_taken_at(item) == "2024-06-15T10:30:00Z"

    def test_exif_datetime_digitized_fallback_when_original_absent(self):
        item = _item(exif={
            "DateTimeDigitized": "2024:06:15 10:30:00",
            "OffsetTimeOriginal": "+00:00",
        })
        assert _parse_taken_at(item) == "2024-06-15T10:30:00Z"

    def test_offset_time_used_when_offset_time_original_absent(self):
        # OffsetTime is the fallback for OffsetTimeOriginal
        item = _item(exif={
            "DateTimeOriginal": "2024:06:15 12:00:00",
            "OffsetTime": "+02:00",
        })
        assert _parse_taken_at(item) == "2024-06-15T10:00:00Z"

    def test_malformed_exif_datetime_falls_through_to_date_created(self):
        item = _item(
            exif={"DateTimeOriginal": "not-a-valid-date"},
            date_created="2024-06-15T10:30:00Z",
        )
        assert _parse_taken_at(item) == "2024-06-15T10:30:00Z"

    def test_empty_exif_dict_falls_through_to_date_created(self):
        item = _item(exif={}, date_created="2024-06-15T10:30:00Z")
        assert _parse_taken_at(item) == "2024-06-15T10:30:00Z"

    def test_date_created_z_suffix_returned_as_utc(self):
        item = _item(date_created="2024-06-15T10:30:00Z")
        assert _parse_taken_at(item) == "2024-06-15T10:30:00Z"

    def test_date_created_with_positive_offset_converted_to_utc(self):
        item = _item(date_created="2024-06-15T12:00:00+02:00")
        assert _parse_taken_at(item) == "2024-06-15T10:00:00Z"

    def test_date_created_with_negative_offset_converted_to_utc(self):
        item = _item(date_created="2024-06-15T08:00:00-05:00")
        assert _parse_taken_at(item) == "2024-06-15T13:00:00Z"

    def test_taken_at_used_when_date_created_none(self):
        item = _item(taken_at="2024-06-15T10:30:00Z")
        assert _parse_taken_at(item) == "2024-06-15T10:30:00Z"

    def test_date_created_takes_priority_over_taken_at(self):
        item = _item(
            date_created="2024-06-15T10:30:00Z",
            taken_at="2024-06-15T09:00:00Z",
        )
        assert _parse_taken_at(item) == "2024-06-15T10:30:00Z"

    def test_all_sources_none_returns_none(self):
        assert _parse_taken_at(_item()) is None

    def test_result_always_has_z_suffix(self):
        item = _item(date_created="2024-06-15T10:30:00+01:00")
        result = _parse_taken_at(item)
        assert result is not None
        assert result.endswith("Z")


# ---------------------------------------------------------------------------
# _resolve_source_app
# ---------------------------------------------------------------------------

class TestResolveSourceApp:

    def test_album_snapchat_exact(self):
        assert _resolve_source_app(_item(album="Snapchat")) == "snapchat"

    def test_album_match_is_case_insensitive(self):
        assert _resolve_source_app(_item(album="SNAPCHAT")) == "snapchat"

    def test_album_instagram_partial_match(self):
        assert _resolve_source_app(_item(album="Instagram Saves")) == "instagram"

    def test_album_whatsapp(self):
        assert _resolve_source_app(_item(album="WhatsApp")) == "whatsapp"

    def test_album_takes_priority_over_camera_make(self):
        assert _resolve_source_app(_item(album="Snapchat", camera_make="Apple")) == "snapchat"

    def test_camera_make_present_no_album_returns_camera(self):
        assert _resolve_source_app(_item(camera_make="Apple")) == "camera"

    def test_camera_make_present_overrides_unknown(self):
        assert _resolve_source_app(_item(camera_make="Canon")) == "camera"

    def test_no_album_no_camera_no_exif_returns_unknown(self):
        assert _resolve_source_app(_item()) == "unknown"

    def test_usercomment_b64_identifies_snapchat(self):
        encoded = b64encode(b"SnapchatApp").decode()
        item = _item(exif={"UserComment": encoded})
        assert _resolve_source_app(item) == "snapchat"

    def test_usercomment_b64_identifies_instagram(self):
        encoded = b64encode(b"instagram").decode()
        item = _item(exif={"UserComment": encoded})
        assert _resolve_source_app(item) == "instagram"

    def test_malformed_usercomment_returns_unknown(self):
        item = _item(exif={"UserComment": "!!!not-base64!!!"})
        assert _resolve_source_app(item) == "unknown"

    def test_usercomment_unrecognised_content_returns_unknown(self):
        encoded = b64encode(b"some random camera metadata").decode()
        item = _item(exif={"UserComment": encoded})
        assert _resolve_source_app(item) == "unknown"


# ---------------------------------------------------------------------------
# _normalise_media_type
# ---------------------------------------------------------------------------

class TestNormaliseMediaType:

    def test_screen_recording(self):
        assert _normalise_media_type(_item(is_screen_recording=True)) == "screen_recording"

    def test_screenshot(self):
        assert _normalise_media_type(_item(is_screenshot=True)) == "screenshot"

    def test_video_from_duration(self):
        assert _normalise_media_type(_item(duration_s=30.5)) == "video"

    def test_photo_is_default(self):
        assert _normalise_media_type(_item()) == "photo"

    def test_screen_recording_beats_screenshot(self):
        item = _item(is_screen_recording=True, is_screenshot=True)
        assert _normalise_media_type(item) == "screen_recording"

    def test_screenshot_beats_video(self):
        item = _item(is_screenshot=True, duration_s=10.0)
        assert _normalise_media_type(item) == "screenshot"


# ---------------------------------------------------------------------------
# _insert_batch
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_table():
    with patch("upload.photos.router.photos_table") as m:
        m.insert.return_value = True
        yield m


@pytest.fixture
def mock_place_id():
    with patch("upload.photos.router.get_place_id", return_value=42) as m:
        yield m


class TestInsertBatch:

    def test_item_without_file_path_not_inserted(self, mock_table, mock_place_id):
        _insert_batch([_item(taken_at="2024-06-15T10:30:00Z")])
        mock_table.insert.assert_not_called()

    def test_item_without_resolvable_timestamp_not_inserted(self, mock_table, mock_place_id):
        _insert_batch([_item(file_path="/photos/img.jpg")])
        mock_table.insert.assert_not_called()

    def test_valid_item_is_inserted(self, mock_table, mock_place_id):
        _insert_batch([_item(file_path="/photos/img.jpg", taken_at="2024-06-15T10:30:00Z")])
        mock_table.insert.assert_called_once()

    def test_duplicate_file_path_not_counted_as_inserted(self, mock_table, mock_place_id):
        mock_table.insert.return_value = False
        _insert_batch([_item(file_path="/photos/img.jpg", taken_at="2024-06-15T10:30:00Z")])
        mock_table.insert.assert_called_once()

    def test_valid_gps_calls_get_place_id(self, mock_table, mock_place_id):
        _insert_batch([_item(
            file_path="/photos/img.jpg",
            taken_at="2024-06-15T10:30:00Z",
            location_lat=51.5,
            location_lon=-0.1,
        )])
        mock_place_id.assert_called_once_with(51.5, -0.1)

    def test_valid_gps_sets_gps_source_and_place_id_on_record(self, mock_table, mock_place_id):
        _insert_batch([_item(
            file_path="/photos/img.jpg",
            taken_at="2024-06-15T10:30:00Z",
            location_lat=51.5,
            location_lon=-0.1,
        )])
        record = mock_table.insert.call_args.args[0]
        assert record.gps_source == "exif"
        assert record.place_id == 42
        assert record.latitude == pytest.approx(51.5)
        assert record.longitude == pytest.approx(-0.1)

    def test_no_gps_does_not_call_get_place_id(self, mock_table, mock_place_id):
        _insert_batch([_item(file_path="/photos/img.jpg", taken_at="2024-06-15T10:30:00Z")])
        mock_place_id.assert_not_called()

    def test_no_gps_sets_gps_source_none(self, mock_table, mock_place_id):
        _insert_batch([_item(file_path="/photos/img.jpg", taken_at="2024-06-15T10:30:00Z")])
        record = mock_table.insert.call_args.args[0]
        assert record.gps_source is None
        assert record.latitude is None
        assert record.longitude is None

    def test_gps_zero_zero_treated_as_missing(self, mock_table, mock_place_id):
        # Shortcuts sends 0.0 for absent GPS — should be treated as null, not a
        # real coordinate (which would be in the Gulf of Guinea).
        _insert_batch([_item(
            file_path="/photos/img.jpg",
            taken_at="2024-06-15T10:30:00Z",
            location_lat=0.0,
            location_lon=0.0,
        )])
        mock_place_id.assert_not_called()
        record = mock_table.insert.call_args.args[0]
        assert record.latitude is None
        assert record.longitude is None

    def test_media_type_derived_from_flags(self, mock_table, mock_place_id):
        _insert_batch([_item(
            file_path="/photos/img.jpg",
            taken_at="2024-06-15T10:30:00Z",
            is_screenshot=True,
        )])
        record = mock_table.insert.call_args.args[0]
        assert record.media_type == "screenshot"

    def test_source_app_derived_from_album(self, mock_table, mock_place_id):
        _insert_batch([_item(
            file_path="/photos/img.jpg",
            taken_at="2024-06-15T10:30:00Z",
            album="Snapchat",
        )])
        record = mock_table.insert.call_args.args[0]
        assert record.source_app == "snapchat"

    def test_taken_at_normalised_to_utc_z(self, mock_table, mock_place_id):
        # Even if Shortcuts sends a timestamp with an offset, the stored value must be UTC+Z
        _insert_batch([_item(
            file_path="/photos/img.jpg",
            taken_at="2024-06-15T12:00:00+02:00",
        )])
        record = mock_table.insert.call_args.args[0]
        assert record.taken_at == "2024-06-15T10:00:00Z"

    def test_raw_exif_passed_through_to_record(self, mock_table, mock_place_id):
        exif = {"Make": "Apple", "Model": "iPhone 15"}
        _insert_batch([_item(
            file_path="/photos/img.jpg",
            taken_at="2024-06-15T10:30:00Z",
            exif=exif,
        )])
        record = mock_table.insert.call_args.args[0]
        assert record.raw_exif == exif

    def test_mixed_batch_only_valid_items_inserted(self, mock_table, mock_place_id):
        items = [
            _item(file_path="/photos/a.jpg", taken_at="2024-06-15T10:30:00Z"),  # valid
            _item(taken_at="2024-06-15T10:30:00Z"),                              # no file_path
            _item(file_path="/photos/b.jpg"),                                    # no timestamp
        ]
        _insert_batch(items)
        assert mock_table.insert.call_count == 1
