"""Business-friendly display labels for report figures.

Canonical CSV values remain unchanged.  These helpers only translate compact
technical labels into readable chart text while retaining SIC codes for audit
traceability.
"""

from __future__ import annotations

import math
import re
from typing import Any


# OSHA's SIC manual names for two-digit major groups.  The upstream hybrid
# model emits labels such as ``SIC 73`` at this level.
SIC_MAJOR_GROUP_NAMES = {
    1: "Agricultural Production—Crops",
    2: "Agricultural Production—Livestock",
    7: "Agricultural Services",
    8: "Forestry",
    9: "Fishing, Hunting & Trapping",
    10: "Metal Mining",
    12: "Coal Mining",
    13: "Oil & Gas Extraction",
    14: "Nonmetallic Minerals Mining",
    15: "Building Construction",
    16: "Heavy Construction",
    17: "Specialty Construction Trades",
    20: "Food Products",
    21: "Tobacco Products",
    22: "Textile Mill Products",
    23: "Apparel & Fabric Products",
    24: "Lumber & Wood Products",
    25: "Furniture & Fixtures",
    26: "Paper Products",
    27: "Printing & Publishing",
    28: "Chemicals & Allied Products",
    29: "Petroleum Refining",
    30: "Rubber & Plastics Products",
    31: "Leather Products",
    32: "Stone, Clay, Glass & Concrete Products",
    33: "Primary Metal Industries",
    34: "Fabricated Metal Products",
    35: "Industrial Machinery & Computer Equipment",
    36: "Electronic & Electrical Equipment",
    37: "Transportation Equipment",
    38: "Instruments, Medical & Optical Goods",
    39: "Miscellaneous Manufacturing",
    40: "Railroad Transportation",
    41: "Passenger Ground Transportation",
    42: "Motor Freight & Warehousing",
    43: "U.S. Postal Service",
    44: "Water Transportation",
    45: "Air Transportation",
    46: "Pipelines, Except Natural Gas",
    47: "Transportation Services",
    48: "Communications",
    49: "Electric, Gas & Sanitary Services",
    50: "Wholesale Trade—Durable Goods",
    51: "Wholesale Trade—Nondurable Goods",
    52: "Building Materials & Garden Supplies",
    53: "General Merchandise Stores",
    54: "Food Stores",
    55: "Automotive Dealers & Service Stations",
    56: "Apparel & Accessory Stores",
    57: "Home Furniture & Equipment Stores",
    58: "Eating & Drinking Places",
    59: "Miscellaneous Retail",
    60: "Depository Institutions",
    61: "Nondepository Credit Institutions",
    62: "Securities & Commodity Services",
    63: "Insurance Carriers",
    64: "Insurance Agents & Brokers",
    65: "Real Estate",
    67: "Holding & Investment Offices",
    70: "Hotels & Lodging",
    72: "Personal Services",
    73: "Business Services",
    75: "Automotive Repair & Parking",
    76: "Miscellaneous Repair Services",
    78: "Motion Pictures",
    79: "Amusement & Recreation Services",
    80: "Health Services",
    81: "Legal Services",
    82: "Educational Services",
    83: "Social Services",
    84: "Museums, Galleries & Gardens",
    86: "Membership Organizations",
    87: "Engineering, Research & Management Services",
    88: "Private Households",
    89: "Miscellaneous Services",
    91: "Executive & General Government",
    92: "Justice & Public Safety",
    93: "Public Finance & Monetary Policy",
    94: "Human Resource Programs Administration",
    95: "Environmental & Housing Administration",
    96: "Economic Programs Administration",
    97: "National Security & International Affairs",
    99: "Nonclassifiable Establishments",
}

FUNNEL_STAGE_LABELS = {
    "raw": "Raw dividend events",
    "cash_positive": "Positive cash dividend",
    "has_core": "Complete core return data",
    "window_contiguous": "Complete event window",
    "benchmark_excluded": "Market benchmark excluded",
}

QUANTILE_LABELS = {
    "Q01": "Lowest 20%",
    "Q02": "Lower-middle 20%",
    "Q03": "Middle 20%",
    "Q04": "Upper-middle 20%",
    "Q05": "Highest 20%",
}


def funnel_stage_label(value: Any) -> str:
    """Translate a canonical funnel stage without changing its stored value."""

    text = str(value)
    return FUNNEL_STAGE_LABELS.get(text, text.replace("_", " ").capitalize())


def quantile_label(value: Any) -> str:
    """Return a plain-language quantile label with the canonical code retained."""

    text = str(value)
    friendly = QUANTILE_LABELS.get(text)
    return f"{friendly}\n({text})" if friendly else text


def _business_title_case(value: str) -> str:
    rendered = value.title()
    rendered = re.sub(r"\bNec\b", "NEC", rendered)
    rendered = re.sub(r"\bSic\b", "SIC", rendered)
    rendered = re.sub(r"\bReits\b", "REITs", rendered)
    return rendered


def business_sector_label(value: Any, taxonomy: str) -> str:
    """Decode sector labels for display while preserving source provenance."""

    text = str(value).strip()
    if taxonomy == "pseudo_sector":
        match = re.fullmatch(r"SIC\s+(\d{1,2})", text, flags=re.IGNORECASE)
        if match:
            code = int(match.group(1))
            name = SIC_MAJOR_GROUP_NAMES.get(code, "Industry Major Group")
            return f"{name} (SIC {code:02d})"
        if text.lower().endswith(" (other)"):
            return "Other " + text[:-8]
        if text.upper() == "OTHER":
            return "Other / unclassified industries"
    return _business_title_case(text)


def event_offset_label(offset: Any) -> str:
    """Convert an integer event-day offset to an unambiguous display label."""

    value = int(offset)
    if value == 0:
        return "Ex-date"
    when = "before" if value < 0 else "after"
    days = abs(value)
    return f"{days} day{'s' if days != 1 else ''}\n{when}"


def _missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return math.isnan(float(value))
    except (TypeError, ValueError):
        return False


def _compact_dollars(value: Any) -> str:
    number = float(value)
    magnitude = abs(number)
    if magnitude >= 1_000_000_000:
        return f"${number / 1_000_000_000:.2f}B"
    if magnitude >= 1_000_000:
        return f"${number / 1_000_000:.2f}M"
    if magnitude >= 1_000:
        return f"${number / 1_000:.1f}K"
    return f"${number:,.0f}"


def boundary_value(value: Any, dimension: str) -> str:
    """Format a quantile boundary in the business unit of its dimension."""

    if _missing(value):
        return ""
    if dimension in ("yield", "volatility"):
        return f"{float(value) * 100.0:.3f}%"
    if dimension == "liquidity":
        return _compact_dollars(value)
    return f"{float(value):.6g}"


def boundary_range(lower: Any, upper: Any, dimension: str) -> str:
    """Describe the canonical (lower, upper] interval in plain language."""

    if _missing(lower):
        return f"up to {boundary_value(upper, dimension)}"
    if _missing(upper):
        return f"above {boundary_value(lower, dimension)}"
    return (
        f"above {boundary_value(lower, dimension)} through "
        f"{boundary_value(upper, dimension)}"
    )
