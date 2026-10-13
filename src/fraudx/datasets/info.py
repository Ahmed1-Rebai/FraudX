"""Static description of a public dataset (provenance, licence, mapping, limits)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetInfo:
    name: str
    title: str
    source_url: str
    kaggle_ref: str
    raw_files: tuple[str, ...]
    license: str
    citation: str
    # (raw column(s), canonical field(s), transformation / note)
    mapping: tuple[tuple[str, str, str], ...]
    limitations: tuple[str, ...]
