"""PDF filename checks for IFU Drawing primary content.

Parses the structured filename of each IFU Drawing's primary PDF and
compares its parts against the object's own metadata (Number, Revision,
language code). Failures are stored in check_results with check_name
"Content Change Required".

Expected filename format:
    {number}_{revision}_{doc_type}_{language}.pdf
    e.g.  12345_AA_IFU_EN.pdf  or  ABC-123_F_PIL_FR.pdf
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from ptc_syncer_ingestion.db import get_objects_by_type, get_pdfs_for_object, save_check_results
from ptc_syncer_ingestion.models import CheckResult
from ptc_syncer_ingestion.registry import register_check

log = logging.getLogger(__name__)

CHECK_NAME = "Content Change Required"
LANG_CHECK_NAME = "IFU Drawing PDF Language"

# How many trailing characters of the extracted text to treat as "the last page"
# when looking for the language code/footer. Approximate — the stored text is one
# whole-document markdown blob with no page boundaries.
TAIL_CHARS = 1200

# ISO 639-1 two-letter language codes (lowercase).
# Source: https://localizely.com/iso-639-1-list/
ISO_639_1_CODES: frozenset[str] = frozenset({
    "aa", "ab", "ae", "af", "ak", "am", "an", "ar", "as", "av", "ay", "az",
    "ba", "be", "bg", "bh", "bi", "bm", "bn", "bo", "br", "bs",
    "ca", "ce", "ch", "co", "cr", "cs", "cu", "cv", "cy",
    "da", "de", "dv", "dz",
    "ee", "el", "en", "eo", "es", "et", "eu",
    "fa", "ff", "fi", "fj", "fo", "fr", "fy",
    "ga", "gd", "gl", "gn", "gu", "gv",
    "ha", "he", "hi", "ho", "hr", "ht", "hu", "hy", "hz",
    "ia", "id", "ie", "ig", "ii", "ik", "io", "is", "it", "iu",
    "ja", "jv",
    "ka", "kg", "ki", "kj", "kk", "kl", "km", "kn", "ko", "kr", "ks",
    "ku", "kv", "kw", "ky",
    "la", "lb", "lg", "li", "ln", "lo", "lt", "lu", "lv",
    "mg", "mh", "mi", "mk", "ml", "mn", "mr", "ms", "mt", "my",
    "na", "nb", "nd", "ne", "ng", "nl", "nn", "no", "nr", "nv", "ny",
    "oc", "oj", "om", "or", "os",
    "pa", "pi", "pl", "ps", "pt",
    "qu",
    "rm", "rn", "ro", "ru", "rw",
    "sa", "sc", "sd", "se", "sg", "si", "sk", "sl", "sm", "sn", "so",
    "sq", "sr", "ss", "st", "su", "sv", "sw",
    "ta", "te", "tg", "th", "ti", "tk", "tl", "tn", "to", "tr", "ts",
    "tt", "tw", "ty",
    "ug", "uk", "ur", "uz",
    "ve", "vi", "vo",
    "wa", "wo",
    "xh",
    "yi", "yo",
    "za", "zh", "zu",
})

# Matches a trailing language code suffix on a drawing number: -EN, -FR, -PL, etc.
_LANG_SUFFIX_RE = re.compile(r"^(.*)-([A-Za-z]{2})$")


def parse_pdf_filename(filename: str) -> dict | None:
    """Parse a PDF filename into its structured parts.

    Expected format: {number}_{revision}_{doc_type}_{language}.pdf

    Returns a dict with keys ``number``, ``revision``, ``doc_type``,
    ``language`` (all strings), or ``None`` if the filename does not
    match the expected structure.
    """
    name = filename
    if name.lower().endswith(".pdf"):
        name = name[:-4]
    parts = name.split("_")
    if len(parts) < 4:
        return None
    return {
        "number": parts[0],
        "revision": parts[1],
        "doc_type": parts[2],
        "language": parts[3],
    }


def _strip_language_suffix(number: str) -> str:
    """Remove a trailing -XX language code from a drawing number.

    'XXXXX-EN' -> 'XXXXX'
    'YYY-FR'   -> 'YYY'
    'ABC123'   -> 'ABC123'  (no suffix, returned unchanged)
    """
    m = _LANG_SUFFIX_RE.match(number)
    return m.group(1) if m else number


def _extract_language_suffix(number: str) -> str | None:
    """Extract the language code from a drawing number, uppercased.

    'XXXXX-EN' -> 'EN'
    'YYY-PL'   -> 'PL'
    'ABC123'   -> None
    """
    m = _LANG_SUFFIX_RE.match(number)
    return m.group(2).upper() if m else None


@register_check("ifu_drawing_pdf_filename")
def run_pdf_filename_checks(conn) -> list[CheckResult]:
    """Check IFU Drawing primary-content filenames against object metadata.

    For each IFU Drawing that has PDF metadata in the ``pdfs`` table:
      1. Number match     — filename number == drawing number (language suffix stripped)
      2. Revision match   — filename revision == drawing Revision
      3. Language valid   — filename language code is a recognised ISO 639-1 code
      4. Language match   — filename language code == language code in drawing number

    Returns a list of CheckResult objects (all results, pass and fail).
    """
    now = datetime.now(timezone.utc).isoformat()
    results: list[CheckResult] = []

    ifu_drawings = get_objects_by_type(conn, "IFU Drawing")
    log.info("Running PDF filename checks for %d IFU Drawings", len(ifu_drawings))

    for drawing in ifu_drawings:
        pdfs = get_pdfs_for_object(conn, drawing.id)
        primary_pdfs = [p for p in pdfs if p.content_role == "primary"]

        if not primary_pdfs:
            log.debug("  No primary PDF metadata for %s — skipping", drawing.number or drawing.id)
            continue

        for pdf in primary_pdfs:
            parsed = parse_pdf_filename(pdf.filename)

            if parsed is None:
                results.append(CheckResult(
                    check_name=CHECK_NAME,
                    source_object_id=drawing.id,
                    target_object_id=pdf.filename,
                    source_attr="Number",
                    target_attr="Filename Format",
                    source_value=drawing.number,
                    target_value=pdf.filename,
                    passed=False,
                    message=(
                        f"FAIL: filename '{pdf.filename}' does not match expected format "
                        "{number}_{revision}_{doc_type}_{language}.pdf"
                    ),
                    checked_at=now,
                ))
                continue

            drawing_number = drawing.number or ""
            drawing_revision = drawing.revision or ""

            # 1. Number match
            number_base = _strip_language_suffix(drawing_number)
            number_match = number_base == parsed["number"]
            results.append(CheckResult(
                check_name=CHECK_NAME,
                source_object_id=drawing.id,
                target_object_id=pdf.filename,
                source_attr="Number",
                target_attr="Number Match",
                source_value=number_base,
                target_value=parsed["number"],
                passed=number_match,
                message=(
                    f"{'PASS' if number_match else 'FAIL'}: "
                    f"drawing number '{number_base}' "
                    f"{'==' if number_match else '!='} filename number '{parsed['number']}'"
                ),
                checked_at=now,
            ))

            # 2. Revision match
            revision_match = drawing_revision == parsed["revision"]
            results.append(CheckResult(
                check_name=CHECK_NAME,
                source_object_id=drawing.id,
                target_object_id=pdf.filename,
                source_attr="Revision",
                target_attr="Revision Match",
                source_value=drawing_revision,
                target_value=parsed["revision"],
                passed=revision_match,
                message=(
                    f"{'PASS' if revision_match else 'FAIL'}: "
                    f"drawing revision '{drawing_revision}' "
                    f"{'==' if revision_match else '!='} filename revision '{parsed['revision']}'"
                ),
                checked_at=now,
            ))

            # 3. Language code validity
            lang_code = parsed["language"].upper()
            lang_valid = lang_code.lower() in ISO_639_1_CODES
            results.append(CheckResult(
                check_name=CHECK_NAME,
                source_object_id=drawing.id,
                target_object_id=pdf.filename,
                source_attr="Number",
                target_attr="Language Code Validity",
                source_value=drawing_number,
                target_value=lang_code,
                passed=lang_valid,
                message=(
                    f"{'PASS' if lang_valid else 'FAIL'}: "
                    f"filename language '{lang_code}' is "
                    f"{'a valid' if lang_valid else 'not a valid'} ISO 639-1 code"
                ),
                checked_at=now,
            ))

            # 4. Language code match against drawing number suffix
            drawing_lang = _extract_language_suffix(drawing_number)
            if drawing_lang is not None:
                lang_match = drawing_lang == lang_code
                results.append(CheckResult(
                    check_name=CHECK_NAME,
                    source_object_id=drawing.id,
                    target_object_id=pdf.filename,
                    source_attr="Number",
                    target_attr="Language Code Match",
                    source_value=drawing_lang,
                    target_value=lang_code,
                    passed=lang_match,
                    message=(
                        f"{'PASS' if lang_match else 'FAIL'}: "
                        f"drawing language '{drawing_lang}' "
                        f"{'==' if lang_match else '!='} filename language '{lang_code}'"
                    ),
                    checked_at=now,
                ))

    return results


def code_token_present(text: str | None, code: str) -> bool:
    """True if ``code`` appears in ``text`` as a standalone token.

    The 2-letter code must not be flanked by other letters, so it matches
    "IFU_EN", "IFU EN", "EN.", "-EN" but not "ENGINE" or "TENET".
    """
    if not text or not code:
        return False
    pattern = r"(?<![A-Za-z])" + re.escape(code) + r"(?![A-Za-z])"
    return re.search(pattern, text, re.IGNORECASE) is not None


@register_check("ifu_drawing_pdf_language")
def run_pdf_language_checks(conn) -> list[CheckResult]:
    """Check that an IFU Drawing PDF's language is consistent across sources.

    The language code parsed from the primary PDF filename is the reference. For
    each IFU Drawing with primary-PDF metadata it verifies the code matches:
      1. Title      — appears as a standalone token in the drawing's Name
      2. Number     — equals the -XX language suffix on the drawing Number
      3. Last page  — appears as a standalone token in the tail of the extracted
                      PDF text (requires 'pdf extract'; skipped if unavailable)
    """
    now = datetime.now(timezone.utc).isoformat()
    results: list[CheckResult] = []

    ifu_drawings = get_objects_by_type(conn, "IFU Drawing")
    log.info("Running PDF language checks for %d IFU Drawings", len(ifu_drawings))

    for drawing in ifu_drawings:
        primary_pdfs = [p for p in get_pdfs_for_object(conn, drawing.id)
                        if p.content_role == "primary"]
        for pdf in primary_pdfs:
            parsed = parse_pdf_filename(pdf.filename)
            if parsed is None:
                continue  # filename format is validated by run_pdf_filename_checks
            code = parsed["language"].upper()

            def _result(target_attr, target_value, passed, message):
                return CheckResult(
                    check_name=LANG_CHECK_NAME,
                    source_object_id=drawing.id,
                    target_object_id=pdf.filename,
                    source_attr="FileName Language",
                    target_attr=target_attr,
                    source_value=code,
                    target_value=target_value,
                    passed=passed,
                    status="skip" if message.startswith("SKIP") else "",
                    message=message,
                    checked_at=now,
                )

            # 1. Title
            name = drawing.name or ""
            title_ok = code_token_present(name, code)
            results.append(_result(
                "Title", name, title_ok,
                f"{'PASS' if title_ok else 'FAIL'}: filename language '{code}' "
                f"{'found in' if title_ok else 'not found in'} title '{name}'",
            ))

            # 2. Number suffix
            num_code = _extract_language_suffix(drawing.number or "")
            number_ok = num_code == code
            results.append(_result(
                "Number", num_code, number_ok,
                f"{'PASS' if number_ok else 'FAIL'}: filename language '{code}' "
                f"{'==' if number_ok else '!='} number suffix '{num_code}'",
            ))

            # 3. Last page of the PDF (tail of extracted text)
            if pdf.extracted_text:
                tail = pdf.extracted_text[-TAIL_CHARS:]
                page_ok = code_token_present(tail, code)
                results.append(_result(
                    "Last Page", "present" if page_ok else "absent", page_ok,
                    f"{'PASS' if page_ok else 'FAIL'}: filename language '{code}' "
                    f"{'found' if page_ok else 'not found'} on last page of PDF",
                ))
            else:
                results.append(_result(
                    "Last Page", None, True,
                    "SKIP: no extracted PDF text; run 'ptc_syncer pdf extract' first",
                ))

    return results


def run_and_save(conn) -> list[CheckResult]:
    """Run PDF filename checks and persist results to the database."""
    results = run_pdf_filename_checks(conn)
    save_check_results(conn, results)
    conn.commit()
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    log.info("Content Change Required: %d passed, %d failed", passed, failed)
    return results
