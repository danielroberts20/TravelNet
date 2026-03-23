"""
test_crontab_tz.py — Tests for crontab_tz.py.

Three layers:
  1. resolve_timezone()      — pure parsing, no I/O
  2. convert_crontab()       — pure text transformation, no I/O
  3. read/write/update_*()   — I/O tests using a tmp_path temp file;
                               CRONTAB_FILE is monkeypatched so the real
                               crontab is never touched.
"""

import pytest
from unittest.mock import patch

import crontab_tz
from crontab_tz import (
    _convert_time_fields,
    convert_crontab,
    read_crontab,
    reset_crontab_timezone,
    resolve_timezone,
    update_crontab_timezone,
    write_crontab,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_CRONTAB = """\
MAILTO=""
# Daily FX fetch on the 2nd
0 2 2 * * /usr/bin/docker exec travelnet sh -c "cd /app && python3 -u -m scheduled_tasks.get_fx"
0 6 * * * /usr/bin/docker exec travelnet sh -c "cd /app && python3 -u -m scheduled_tasks.send_warn_error_log"
30 2 2 * * /usr/bin/docker exec travelnet sh -c "cd /app && python3 -u -m scheduled_tasks.backfill_gbp"
0 0 1 * * /usr/bin/docker exec travelnet sh -c "cd /app && python3 -u -m scheduled_tasks.reset_fx_api_usage"
50 5 * * 1 /usr/bin/docker exec travelnet sh -c "cd /app && python3 -u -m scheduled_tasks.check_health_gaps"
@reboot /home/dan/start.sh
"""


@pytest.fixture
def crontab_file(tmp_path):
    """Temp file pre-populated with SAMPLE_CRONTAB; CRONTAB_FILE is patched to point at it."""
    f = tmp_path / "crontab_dan"
    f.write_text(SAMPLE_CRONTAB)
    with patch.object(crontab_tz, "CRONTAB_FILE", str(f)):
        yield f


# ---------------------------------------------------------------------------
# 1. resolve_timezone
# ---------------------------------------------------------------------------

class TestResolveTimezone:
    def test_utc_offset_positive(self):
        label, offset = resolve_timezone("+10")
        assert offset == 600
        assert label == "+10"

    def test_utc_offset_with_minutes(self):
        _, offset = resolve_timezone("+05:30")
        assert offset == 330

    def test_utc_offset_negative(self):
        _, offset = resolve_timezone("-0500")
        assert offset == -300

    def test_utc_offset_zero(self):
        _, offset = resolve_timezone("+00")
        assert offset == 0

    def test_iana_name(self):
        label, offset = resolve_timezone("Etc/UTC")
        assert offset == 0
        assert "Etc/UTC" in label

    def test_iana_fixed_offset(self):
        # Asia/Tokyo is always UTC+9, no DST
        _, offset = resolve_timezone("Asia/Tokyo")
        assert offset == 540

    def test_abbreviation_jst(self):
        _, offset = resolve_timezone("JST")
        assert offset == 540

    def test_abbreviation_case_insensitive(self):
        _, offset_upper = resolve_timezone("JST")
        _, offset_lower = resolve_timezone("jst")
        assert offset_upper == offset_lower

    def test_abbreviation_utc(self):
        _, offset = resolve_timezone("UTC")
        assert offset == 0

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Cannot resolve timezone"):
            resolve_timezone("NOTAZONE")

    def test_label_contains_iana_name(self):
        label, _ = resolve_timezone("America/New_York")
        assert "America/New_York" in label

    def test_label_contains_utc_offset(self):
        label, _ = resolve_timezone("Asia/Tokyo")
        assert "UTC+09:00" in label


# ---------------------------------------------------------------------------
# 2. _convert_time_fields (unit tests for the core arithmetic)
# ---------------------------------------------------------------------------

class TestConvertTimeFields:
    def test_no_change_when_offset_zero(self):
        m, h, dom, dow, delta, warn = _convert_time_fields("0", "6", "*", "*", 0)
        assert (m, h, delta, warn) == ("0", "6", 0, None)

    def test_simple_positive_offset(self):
        # Pi=GMT(0), user=EST(-300): offset = 0 - (-300) = +300 min = +5h
        m, h, *_ = _convert_time_fields("0", "6", "*", "*", 300)
        assert (m, h) == ("0", "11")

    def test_simple_negative_offset(self):
        # user is 3h ahead of Pi: offset = 0 - 180 = -180
        m, h, *_ = _convert_time_fields("0", "12", "*", "*", -180)
        assert (m, h) == ("0", "9")

    def test_minute_preserved(self):
        m, h, *_ = _convert_time_fields("30", "5", "*", "*", 300)
        assert (m, h) == ("30", "10")

    def test_minute_carry(self):
        # 02:50 + 90min offset = 04:20
        m, h, *_ = _convert_time_fields("50", "2", "*", "*", 90)
        assert (m, h) == ("20", "4")

    def test_wildcard_hour_unchanged(self):
        # * hour means "every hour" — pass through untouched
        m, h, dom, dow, delta, warn = _convert_time_fields("0", "*", "*", "*", 300)
        assert (m, h, delta, warn) == ("0", "*", 0, None)

    def test_wildcard_minute_preserved(self):
        m, h, *_ = _convert_time_fields("*", "6", "*", "*", 300)
        assert (m, h) == ("*", "11")

    def test_midnight_wrap_forward(self):
        # 23:00 + 3h → 02:00 next day
        m, h, dom, dow, delta, warn = _convert_time_fields("0", "23", "*", "*", 180)
        assert (m, h) == ("0", "2")
        assert delta == 1

    def test_midnight_wrap_backward(self):
        # 01:00 - 3h → 22:00 previous day
        m, h, dom, dow, delta, warn = _convert_time_fields("0", "1", "*", "*", -180)
        assert (m, h) == ("0", "22")
        assert delta == -1

    def test_dow_adjusted_on_forward_wrap(self):
        # Monday job (1) shifts forward past midnight → Tuesday (2)
        _, _, _, new_dow, delta, _ = _convert_time_fields("0", "23", "*", "1", 180)
        assert delta == 1
        assert new_dow == "2"

    def test_dow_adjusted_on_backward_wrap(self):
        # Monday job (1) shifts backward past midnight → Sunday (0)
        _, _, _, new_dow, delta, _ = _convert_time_fields("0", "1", "*", "1", -180)
        assert delta == -1
        assert new_dow == "0"

    def test_dow_sunday_wraps_to_saturday(self):
        # Sunday (0) - 1 day → Saturday (6)
        _, _, _, new_dow, _, _ = _convert_time_fields("0", "1", "*", "0", -180)
        assert new_dow == "6"

    def test_dow_wildcard_unchanged_on_wrap(self):
        _, _, _, new_dow, delta, _ = _convert_time_fields("0", "23", "*", "*", 180)
        assert delta == 1
        assert new_dow == "*"  # wildcard stays wildcard

    def test_specific_dom_generates_warning_on_wrap(self):
        _, _, _, _, delta, warn = _convert_time_fields("0", "23", "2", "*", 180)
        assert delta == 1
        assert warn is not None
        assert "2" in warn

    def test_no_warning_when_dom_wildcard_and_wrap(self):
        _, _, _, _, delta, warn = _convert_time_fields("0", "23", "*", "*", 180)
        assert delta == 1
        assert warn is None

    def test_complex_hour_field_skipped(self):
        m, h, _, _, _, warn = _convert_time_fields("0", "1-5", "*", "*", 300)
        assert (m, h) == ("0", "1-5")  # unchanged
        assert warn is not None

    def test_complex_minute_field_skipped(self):
        m, h, _, _, _, warn = _convert_time_fields("*/15", "6", "*", "*", 300)
        assert (m, h) == ("*/15", "6")
        assert warn is not None

    def test_fractional_offset_india(self):
        # IST = UTC+5:30 = +330 min; Pi=UTC+0 → offset = 0 - 330 = -330
        # 06:00 - 5h30m = 00:30
        m, h, *_ = _convert_time_fields("0", "6", "*", "*", -330)
        assert (m, h) == ("30", "0")


# ---------------------------------------------------------------------------
# 3. convert_crontab (full-text transformation)
# ---------------------------------------------------------------------------

class TestConvertCrontab:
    def _convert(self, text, user_offset, pi_offset=0):
        return convert_crontab(text, user_offset, pi_offset)

    def test_comments_pass_through(self):
        text = "# this is a comment\n0 6 * * * cmd\n"
        new, _ = self._convert(text, -300)
        assert "# this is a comment" in new

    def test_blank_lines_pass_through(self):
        text = "\n0 6 * * * cmd\n\n"
        new, _ = self._convert(text, -300)
        assert new.startswith("\n")

    def test_variable_assignment_passes_through(self):
        text = 'MAILTO=""\n0 6 * * * cmd\n'
        new, _ = self._convert(text, -300)
        assert 'MAILTO=""' in new

    def test_at_special_passes_through(self):
        text = "@reboot /home/dan/start.sh\n0 6 * * * cmd\n"
        new, _ = self._convert(text, -300)
        assert "@reboot /home/dan/start.sh" in new

    def test_simple_conversion(self):
        text = "0 6 * * * /usr/bin/docker exec foo\n"
        new, changes = self._convert(text, user_offset=-300, pi_offset=0)
        assert "0 11 * * *" in new
        assert len(changes) == 1
        assert changes[0]['changed'] is True

    def test_no_change_when_same_offset(self):
        text = "0 6 * * * cmd\n"
        new, changes = self._convert(text, user_offset=0, pi_offset=0)
        assert "0 6 * * *" in new
        assert not any(c['changed'] for c in changes)

    def test_multiple_lines_all_converted(self):
        text = (
            "0 6 * * * cmd_a\n"
            "0 3 2 * * cmd_b\n"
            "50 5 * * 1 cmd_c\n"
        )
        new, changes = self._convert(text, user_offset=-300, pi_offset=0)
        assert "0 11 * * * cmd_a" in new
        assert "0 8 2 * * cmd_b" in new
        assert "50 10 * * 1 cmd_c" in new
        assert len(changes) == 3

    def test_change_record_fields(self):
        text = "0 6 * * * cmd\n"
        _, changes = self._convert(text, user_offset=-300, pi_offset=0)
        c = changes[0]
        assert 'original' in c
        assert 'converted' in c
        assert 'changed' in c
        assert 'day_delta' in c
        assert 'warning' in c

    def test_command_preserved_verbatim(self):
        cmd = '/usr/bin/docker exec travelnet sh -c "cd /app && python3 -u -m foo"'
        text = f"0 6 * * * {cmd}\n"
        new, _ = self._convert(text, user_offset=-300, pi_offset=0)
        assert cmd in new

    def test_output_ends_with_newline(self):
        text = "0 6 * * * cmd\n"
        new, _ = self._convert(text, user_offset=-300, pi_offset=0)
        assert new.endswith('\n')

    def test_bst_to_est_conversion(self):
        # Pi in BST (UTC+1, +60), user in EST (UTC-5, -300)
        # offset = 60 - (-300) = 360 min = +6h
        # 06:00 → 12:00 BST (= 06:00 EST ✓)
        text = "0 6 * * * cmd\n"
        new, _ = self._convert(text, user_offset=-300, pi_offset=60)
        assert "0 12 * * *" in new

    def test_roundtrip(self):
        # Convert GMT→EST then EST→GMT should give back the original
        text = "0 6 * * * cmd\n30 2 2 * * cmd2\n"
        step1, _ = self._convert(text, user_offset=-300, pi_offset=0)
        step2, _ = self._convert(step1, user_offset=0, pi_offset=-300)
        # Strip trailing whitespace differences for comparison
        orig_lines = {l.strip() for l in text.splitlines() if l.strip()}
        final_lines = {l.strip() for l in step2.splitlines() if l.strip()}
        assert orig_lines == final_lines


# ---------------------------------------------------------------------------
# 4. read_crontab / write_crontab (temp file, never touches real crontab)
# ---------------------------------------------------------------------------

class TestCrontabIO:
    def test_read_returns_file_contents(self, crontab_file):
        content = read_crontab()
        assert content == SAMPLE_CRONTAB

    def test_read_returns_empty_for_missing_file(self, tmp_path):
        missing = tmp_path / "nonexistent"
        with patch.object(crontab_tz, "CRONTAB_FILE", str(missing)):
            assert read_crontab() == ""

    def test_write_updates_file(self, crontab_file):
        new_content = "0 9 * * * new_cmd\n"
        write_crontab(new_content)
        assert crontab_file.read_text() == new_content

    def test_write_then_read_roundtrip(self, crontab_file):
        content = "# updated\n0 7 * * * cmd\n"
        write_crontab(content)
        assert read_crontab() == content


# ---------------------------------------------------------------------------
# 5. update_crontab_timezone (full pipeline, temp file)
# ---------------------------------------------------------------------------

class TestUpdateCrontabTimezone:
    def test_returns_summary_dict(self, crontab_file):
        result = update_crontab_timezone("+09:00")
        assert 'timezone_label' in result
        assert 'pi_offset_min' in result
        assert 'user_offset_min' in result
        assert 'changes' in result

    def test_file_is_actually_updated(self, crontab_file):
        original = crontab_file.read_text()
        update_crontab_timezone("+09:00")
        updated = crontab_file.read_text()
        assert updated != original

    def test_original_restored_after_reverse(self, crontab_file):
        # Roundtrip: Pi(UTC+0) → JST(+540) → UTC+0.
        # To reverse, the second call must treat the Pi's "current" offset as the
        # previous user timezone (+540), mirroring the convert_crontab roundtrip formula:
        #   step1: offset = pi(0) - user(+540) = -540  →  02:00 → 17:00
        #   step2: offset = pi(+540) - user(0) = +540   →  17:00 → 02:00
        original = crontab_file.read_text()
        with patch.object(crontab_tz, "get_pi_utc_offset_minutes", return_value=0):
            update_crontab_timezone("+09:00")
        with patch.object(crontab_tz, "get_pi_utc_offset_minutes", return_value=540):
            update_crontab_timezone("+00:00")
        restored = crontab_file.read_text()
        orig_lines = [l for l in original.splitlines() if l.strip()]
        restored_lines = [l for l in restored.splitlines() if l.strip()]
        assert orig_lines == restored_lines

    def test_invalid_timezone_raises(self, crontab_file):
        with pytest.raises(ValueError):
            update_crontab_timezone("NOTREAL")

    def test_file_unchanged_on_invalid_timezone(self, crontab_file):
        original = crontab_file.read_text()
        with pytest.raises(ValueError):
            update_crontab_timezone("NOTREAL")
        assert crontab_file.read_text() == original

    def test_utc_offset_string_accepted(self, crontab_file):
        result = update_crontab_timezone("+1000")
        assert result['user_offset_min'] == 600

    def test_abbreviation_accepted(self, crontab_file):
        result = update_crontab_timezone("JST")
        assert result['user_offset_min'] == 540

    def test_backup_created_on_conversion(self, crontab_file):
        original = crontab_file.read_text()
        update_crontab_timezone("+09:00")
        backup = crontab_file.with_suffix('.bak')
        assert backup.exists()
        assert backup.read_text() == original

    def test_backup_overwritten_on_second_conversion(self, crontab_file):
        update_crontab_timezone("+09:00")
        after_first = crontab_file.read_text()
        update_crontab_timezone("+05:30")
        backup = crontab_file.with_suffix('.bak')
        assert backup.read_text() == after_first

    def test_reset_restores_backup(self, crontab_file):
        original = crontab_file.read_text()
        update_crontab_timezone("+09:00")
        reset_crontab_timezone()
        assert crontab_file.read_text() == original

    def test_reset_raises_when_no_backup(self, crontab_file):
        with pytest.raises(RuntimeError, match="No backup found"):
            reset_crontab_timezone()

    def test_reset_raises_without_crontab_file(self, tmp_path):
        with patch.object(crontab_tz, "CRONTAB_FILE", None):
            with pytest.raises(RuntimeError, match="CRONTAB_FILE"):
                reset_crontab_timezone()

    def test_same_timezone_skipped(self, crontab_file):
        update_crontab_timezone("+09:00")
        after_first = crontab_file.read_text()
        result = update_crontab_timezone("+09:00")
        assert result['skipped'] is True
        assert crontab_file.read_text() == after_first  # file untouched

    def test_different_timezone_not_skipped(self, crontab_file):
        update_crontab_timezone("+09:00")
        result = update_crontab_timezone("+05:30")
        assert result['skipped'] is False

    def test_skipped_result_has_empty_changes(self, crontab_file):
        update_crontab_timezone("+09:00")
        result = update_crontab_timezone("+09:00")
        assert result['changes'] == []

    def test_backup_not_overwritten_when_skipped(self, crontab_file):
        original = crontab_file.read_text()
        update_crontab_timezone("+09:00")
        update_crontab_timezone("+09:00")  # skipped — backup should still be original
        backup = crontab_file.with_suffix('.bak')
        assert backup.read_text() == original
