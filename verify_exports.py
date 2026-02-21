#!/usr/bin/env python3
"""
Exported File Verifier
======================
Compares a batch of downloaded CSV files against a known set of reference
definitions (file names, column headers, and cell-level content rules).

Usage:
    python verify_exports.py <downloaded_folder>

    If <downloaded_folder> is omitted, defaults to:
        ./downloaded exported files

The reference definitions live inside this script in REFERENCE_FILES.
Each entry specifies:
  - folder / filename
  - expected column headers
  - per-row, per-column expected values (exact string OR regex pattern)

Dynamic data rules
------------------
Columns whose values change between runs are matched with regex patterns
instead of exact strings.  The following pattern tokens are used:

  DATETIME       dd-Mon-yyyy HH:MM:SS   e.g. 19-Feb-2026 11:55:33
  DATE_SLASH     dd/mm/yyyy             e.g. 24/02/2000
  DATE_ONLY      dd-Mon-yyyy HH:MM:SS   (same as DATETIME, for date-only cols)
  INTEGER        sequence of digits      e.g. 255529
  NUMERIC_ID     same as INTEGER
  ANY            any string (including empty)
  EMPTY          must be empty
  NONEMPTY       at least one character

These are expanded into proper regex when the script runs.
"""

import csv
import io
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Regex building blocks
# ─────────────────────────────────────────────────────────────────────────────

# Matches: dd-Mon-yyyy HH:MM:SS  (e.g. 20-Feb-2026 14:55:06)
_DATETIME_RE = r"\d{1,2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}"

# Matches: dd/mm/yyyy  (e.g. 24/02/2000)
_DATE_SLASH_RE = r"\d{1,2}/\d{2}/\d{4}"

# Matches a positive integer (one or more digits)
_INTEGER_RE = r"\d+"

# Pattern token → regex mapping
PATTERN_MAP = {
    "DATETIME":    _DATETIME_RE,
    "DATE_ONLY":   _DATETIME_RE,
    "DATE_SLASH":  _DATE_SLASH_RE,
    "INTEGER":     _INTEGER_RE,
    "NUMERIC_ID":  _INTEGER_RE,
    "ANY":         r".*",
    "EMPTY":       r"^$",
    "NONEMPTY":    r".+",
}

# ─────────────────────────────────────────────────────────────────────────────
# Placeholder / pseudo-blank detection
# ─────────────────────────────────────────────────────────────────────────────
# These patterns catch values that *look* like they were meant to be blank or
# are serialisation artefacts rather than genuine data.

PLACEHOLDER_PATTERNS: list[tuple[re.Pattern, str]] = [
    # JavaScript serialisation bug
    (re.compile(r"\[object Object\]"),      "JavaScript [object Object] — serialisation bug"),
    (re.compile(r"\[object .+\]"),          "JavaScript [object ...] — serialisation bug"),
    # Whitespace-only strings pretending to be blank
    (re.compile(r"^\s+$"),                  "Whitespace-only value (should be truly empty)"),
    # Common programmatic null / placeholder strings
    (re.compile(r"^null$", re.I),           "Literal 'null' — likely a code artifact"),
    (re.compile(r"^undefined$", re.I),      "Literal 'undefined' — likely a code artifact"),
    (re.compile(r"^NaN$"),                  "Literal 'NaN' — likely a code artifact"),
    (re.compile(r"^None$"),                 "Literal 'None' — likely a Python artifact"),
    (re.compile(r"^#N/A$", re.I),           "Spreadsheet error value '#N/A'"),
    (re.compile(r"^#REF!$", re.I),          "Spreadsheet error value '#REF!'"),
    (re.compile(r"^#VALUE!$", re.I),        "Spreadsheet error value '#VALUE!'"),
    (re.compile(r"^#DIV/0!$", re.I),        "Spreadsheet error value '#DIV/0!'"),
]


def _compile_pattern(token_or_literal: str) -> re.Pattern:
    """Return a compiled regex.

    If *token_or_literal* is a key in PATTERN_MAP it is expanded;
    otherwise the string is treated as a literal (escaped) match.
    """
    if token_or_literal in PATTERN_MAP:
        return re.compile(rf"^{PATTERN_MAP[token_or_literal]}$")
    # Treat as exact literal
    return re.compile(rf"^{re.escape(token_or_literal)}$")


# ─────────────────────────────────────────────────────────────────────────────
# Reference file definitions
# ─────────────────────────────────────────────────────────────────────────────
# Structure:
#   {
#       "folder/filename.csv": {
#           "headers": ["col1", "col2", ...],
#           "rows": [
#               ["val_or_PATTERN", "val_or_PATTERN", ...],   # row 1
#               ...
#           ]
#       }
#   }
#
# For columns that hold dynamic data the special tokens above are used.
# For columns that can be empty OR hold a datetime, use "ANY".
# ─────────────────────────────────────────────────────────────────────────────

REFERENCE_FILES = {
    # ── Customer ─────────────────────────────────────────────────────────
    "Customer/Customer barebone.csv": {
        "headers": [
            "Created Date","ID","Customer Name","Reference Code","Partner Name",
            "Trading As","Created By","Action Owner","Status","Contact Phone",
            "Contact Name","Contact Email","KYC Completed At","Registered Country",
            "Registration Number","ABN","ACN","UK Company Number","Legal Entity Name",
            "Entity Type","Verified","OCDD Enabled","Has PEP/Sanctions?",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Last OCDD Outcome","Relationship",
            "OCDD Workflows","Risk Classes","RECORD-Annual Turnover",
            "RECORD-Can you see this?","RECORD-Country?","RECORD-Field 1",
        ],
        "rows": [[
            "DATETIME","INTEGER","MVSI PTY LTD","ANY","MVSI",
            "MVSI PTY LTD","ANY","ANY","Active","ANY",
            "ANY","ANY","DATETIME","Australia",
            "ANY","ANY","ANY","ANY","MVSI PTY LTD",
            "Australian Private Company","Yes","Yes","No",
            "DATETIME","DATETIME","DATETIME",
            "DATETIME","DATETIME","Pass","Customer",
            "Low Risk","Low Risk","ANY","ANY","ANY","Testing",
        ]],
    },
    "Customer/Customer Tags.csv": {
        "headers": [
            "Created Date","ID","Customer Name","Reference Code","Partner Name",
            "Trading As","Created By","Action Owner","Status","Contact Phone",
            "Contact Name","Contact Email","KYC Completed At","Registered Country",
            "Registration Number","ABN","ACN","UK Company Number","Legal Entity Name",
            "Entity Type","Verified","OCDD Enabled","Has PEP/Sanctions?",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Last OCDD Outcome","Relationship",
            "OCDD Workflows","Risk Classes","RECORD-Annual Turnover",
            "RECORD-Can you see this?","RECORD-Country?","RECORD-Field 1",
        ],
        "rows": [[
            "DATETIME","INTEGER","MVSI PTY LTD","ANY","MVSI",
            "MVSI PTY LTD","ANY","ANY","Active","ANY",
            "ANY","ANY","DATETIME","Australia",
            "ANY","ANY","ANY","ANY","MVSI PTY LTD",
            "Australian Private Company","Yes","Yes","No",
            "DATETIME","DATETIME","DATETIME",
            "DATETIME","DATETIME","Pass","Customer",
            "Low Risk","Low Risk","ANY","ANY","ANY","Testing",
        ]],
    },

    # ── Individuals ──────────────────────────────────────────────────────
    "Individuals/Individual Details - tags.csv": {
        "headers": [
            "Created Date","Individual ID","Full Name","Email Address",
            "Date of Birth","Gender","Partner Name","Phone Number",
            "PEP/Sanctions?","Countries of Citizenship","Residential Address",
            "Verified","Organisation ID","Organisation Name","Relationship",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Action Owner",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY",
            "DATE_SLASH","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","INTEGER","ANY","ANY",
            "DATETIME","DATETIME","DATETIME",
            "ANY","ANY","ANY",
        ]],
    },
    "Individuals/Individual Details -Individuals and relationships.csv": {
        "headers": [
            "Created Date","Individual ID","Full Name","Email Address",
            "Date of Birth","Gender","Partner Name","Phone Number",
            "PEP/Sanctions?","Countries of Citizenship","Residential Address",
            "Verified","Organisation ID","Organisation Name","Relationship",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Action Owner",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY",
            "DATE_SLASH","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","INTEGER","ANY","ANY",
            "DATETIME","DATETIME","DATETIME",
            "ANY","ANY","ANY",
        ]],
    },
    "Individuals/Individual Details Individuals.csv": {
        "headers": [
            "Created Date","Individual ID","Full Name","Email Address",
            "Date of Birth","Gender","Partner Name","Phone Number",
            "PEP/Sanctions?","Countries of Citizenship","Residential Address",
            "Verified","Subscription Started At","Last Renewal At",
            "Next Renewal At","OCDD Last Run","OCDD Next Run","Action Owner",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY",
            "DATE_SLASH","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","DATETIME","DATETIME",
            "DATETIME","ANY","ANY","ANY",
        ]],
    },

    # ── My company Configuration Data sources ────────────────────────────
    "My company Configuration Data sources/External Data Source Details - barebone.csv": {
        "headers": ["Data Source ID","Name","Description","Last Modified At","Created At"],
        "rows": [["INTEGER","New Data Source","ANY","DATETIME","DATETIME"]],
    },

    # ── My company Configuration System Templates ────────────────────────
    "My company Configuration System Templates/Communication Details - barebone.csv": {
        "headers": [
            "Created Date","Label","Description","Package ID","Package",
            "Verification Workflow ID","Verification Workflow","Partner ID",
            "Partner","Type","Disabled",
        ],
        # This file has 48 data rows – we validate headers + row count only
        # because the rows are static config and don't change between runs.
        # Use "ANY" for every cell so format is validated but content is flexible.
        "rows": "ANY_ROWS",  # special sentinel: skip per-cell checks
        "expected_row_count": 48,
    },

    # ── My company Configuration System sources ──────────────────────────
    "My company Configuration System sources/External Data Source Details - barebone.csv": {
        "headers": ["Data Source ID","Name","Description","Last Modified At","Created At"],
        "rows": [["INTEGER","New Internal Data Source","ANY","DATETIME","DATETIME"]],
    },

    # ── My company Portfolio Risk Customer Maintenance ────────────────────
    "My company Portfolio Risk Customer Maintenance/Customer Details - Tags.csv": {
        "headers": [
            "Created Date","ID","Customer Name","Reference Code","Partner Name",
            "Trading As","Created By","Action Owner","Status","Contact Phone",
            "Contact Name","Contact Email","KYC Completed At","Registered Country",
            "Registration Number","ABN","ACN","UK Company Number","Legal Entity Name",
            "Entity Type","Verified","OCDD Enabled","Has PEP/Sanctions?",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Last OCDD Outcome","Relationship",
            "OCDD Workflows","Risk Classes","RECORD-Annual Turnover",
            "RECORD-Can you see this?","RECORD-Country?","RECORD-Field 1",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY","ANY",
            "ANY","ANY","ANY","Active","ANY",
            "ANY","ANY","DATETIME","Australia",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","Yes","Yes","No",
            "DATETIME","DATETIME","DATETIME",
            "ANY","DATETIME","ANY","Customer",
            "Low Risk","Low Risk","ANY","ANY","ANY","Testing",
        ]],
    },
    "My company Portfolio Risk Customer Maintenance/Customer Details - barebone.csv": {
        "headers": [
            "Created Date","ID","Customer Name","Reference Code","Partner Name",
            "Trading As","Created By","Action Owner","Status","Contact Phone",
            "Contact Name","Contact Email","KYC Completed At","Registered Country",
            "Registration Number","ABN","ACN","UK Company Number","Legal Entity Name",
            "Entity Type","Verified","OCDD Enabled","Has PEP/Sanctions?",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Last OCDD Outcome","Relationship",
            "OCDD Workflows","Risk Classes","RECORD-Annual Turnover",
            "RECORD-Can you see this?","RECORD-Country?","RECORD-Field 1",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY","ANY",
            "ANY","ANY","ANY","Active","ANY",
            "ANY","ANY","DATETIME","Australia",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","Yes","Yes","No",
            "DATETIME","DATETIME","DATETIME",
            "ANY","DATETIME","ANY","Customer",
            "Low Risk","Low Risk","ANY","ANY","ANY","Testing",
        ]],
    },

    # ── My company Portfolio Risk OCDD Workflows ─────────────────────────
    "My company Portfolio Risk OCDD Workflows/OCDD Workflow Details - Barebone.csv": {
        "headers": [
            "OCDD Workflow ID","Name","Description","Interval","Customers",
            "Individuals","Risk Classes","Created By","Created Date","Partner","Checks",
        ],
        "rows": [[
            "INTEGER","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","DATETIME","MVSI","ANY",
        ]],
    },

    # ── My company Portfolio Risk Related Individuals ─────────────────────
    "My company Portfolio Risk Related Individuals/Individual Details - individual and relationship.csv": {
        "headers": [
            "Created Date","Individual ID","Full Name","Email Address",
            "Date of Birth","Gender","Partner Name","Phone Number",
            "PEP/Sanctions?","Countries of Citizenship","Residential Address",
            "Verified","Organisation ID","Organisation Name","Relationship",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Action Owner",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY",
            "DATE_SLASH","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","INTEGER","ANY","ANY",
            "DATETIME","DATETIME","DATETIME",
            "ANY","ANY","ANY",
        ]],
    },
    "My company Portfolio Risk Related Individuals/Individual Details - individuals and relationshiop tags.csv": {
        "headers": [
            "Created Date","Individual ID","Full Name","Email Address",
            "Date of Birth","Gender","Partner Name","Phone Number",
            "PEP/Sanctions?","Countries of Citizenship","Residential Address",
            "Verified","Organisation ID","Organisation Name","Relationship",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Action Owner",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY",
            "DATE_SLASH","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","INTEGER","ANY","ANY",
            "DATETIME","DATETIME","DATETIME",
            "ANY","ANY","ANY",
        ]],
    },
    "My company Portfolio Risk Related Individuals/Individual Details - individuals tag.csv": {
        "headers": [
            "Created Date","Individual ID","Full Name","Email Address",
            "Date of Birth","Gender","Partner Name","Phone Number",
            "PEP/Sanctions?","Countries of Citizenship","Residential Address",
            "Verified","Subscription Started At","Last Renewal At",
            "Next Renewal At","OCDD Last Run","OCDD Next Run","Action Owner",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY",
            "DATE_SLASH","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","DATETIME","DATETIME",
            "DATETIME","ANY","ANY","ANY",
        ]],
    },
    "My company Portfolio Risk Related Individuals/Individual Details - individuals.csv": {
        "headers": [
            "Created Date","Individual ID","Full Name","Email Address",
            "Date of Birth","Gender","Partner Name","Phone Number",
            "PEP/Sanctions?","Countries of Citizenship","Residential Address",
            "Verified","Subscription Started At","Last Renewal At",
            "Next Renewal At","OCDD Last Run","OCDD Next Run","Action Owner",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY",
            "DATE_SLASH","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","DATETIME","DATETIME",
            "DATETIME","ANY","ANY","ANY",
        ]],
    },

    # ── My company Portfolio Risk Risk Classes ────────────────────────────
    "My company Portfolio Risk Risk Classes/Risk Class Details barebone.csv": {
        "headers": [
            "Risk Class ID","Name","Description","Customers","Individuals",
            "OCDD Workflows","Default Risk Class","Created By","Created Date","Partner",
        ],
        "rows": [[
            "INTEGER","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","DATETIME","MVSI",
        ]],
    },

    # ── My company Regulatory & Legal Contacts ───────────────────────────
    "My company Regulatory & Legal Contacts/Communication Details - barebone.csv": {
        "headers": [
            "Created Date","Label","Description","Package ID","Package",
            "Verification Workflow ID","Verification Workflow","Partner ID",
            "Partner","Type","Disabled",
        ],
        "rows": [[
            "DATETIME","ANY","ANY","ANY","ANY",
            "ANY","ANY","INTEGER","MVSI","ANY","ANY",
        ]],
    },

    # ── My company Regulatory & Legal ID Rules ───────────────────────────
    "My company Regulatory & Legal ID Rules/ID Rule Details - barebones.csv": {
        "headers": ["Created Date","Rule ID","Name","Description"],
        "rows": [["DATETIME","INTEGER","ANY","ANY"]],
    },

    # ── My company Regulatory & Legal Related Individuals ────────────────
    "My company Regulatory & Legal Related Individuals/RI Rules Details - barebones.csv": {
        "headers": ["Rule ID","Name","Country","Entity Type","Description","Status","Created Date"],
        "rows": [["INTEGER","ANY","ANY","ANY","ANY","ANY","DATETIME"]],
    },

    # ── My company Regulatory & Legal Workflows ──────────────────────────
    "My company Regulatory & Legal Workflows/Verification Workflows Details - barebone.csv": {
        "headers": ["ID","Name","Type","Description","Partner","Default Country","Created Date"],
        "rows": [["INTEGER","ANY","ANY","ANY","MVSI","ANY","DATETIME"]],
    },

    # ── My company Tags ──────────────────────────────────────────────────
    "My company Tags/Tag Details - barebone.csv": {
        "headers": [
            "Name","Description","Type","System Tag","Tagged Items",
            "Triggers","Approvals","Packages","Created Date",
        ],
        "rows": [["ANY","ANY","ANY","ANY","ANY","ANY","ANY","ANY","DATETIME"]],
    },

    # ── OCDD ─────────────────────────────────────────────────────────────
    "OCDD/OCDDRun Details barebone.csv": {
        "headers": [
            "OCDD Run ID","Created Date","OCDD Organisation Name",
            "OCDD Organisation Id","OCDD Organisation Reference","Action Owner",
            "OCDD Individual Name","OCDD Individual Id","OCDD Individual Reference",
            "OCDD Workflow Name","OCDD Outcome","Check Name","Check Type",
            "Check Organisation Name","Check Person Name","Check Outcome",
            "Action Required",
        ],
        # Multiple rows – validate headers only; row content is dynamic
        "rows": "ANY_ROWS",
        "min_row_count": 1,
    },

    # ── Offer ────────────────────────────────────────────────────────────
    "Offer/Offer - barebone.csv": {
        "headers": [
            "Created Date","Offer ID","Package","Customer ID",
            "Customer Organisation Name","Sales Owner","Action Owner",
            "Company Name","Contact Name","Contact Email","Contact Number",
            "Customer Reference","Organisation Nickname","Individual Nickname",
            "Stage","Status","On Hold","Last Actioned By","Completed By",
            "Legal Entity Name","ABN","ACN","Registration Number",
            "UK Company Number","Avg Card Ticket Size","Annual Turnover",
            "Annual CC Turnover","Terminals QTY","Ecommerce (Y/N)",
            "Primary Partner","Billing Partner","Locale","Last Note Date",
            "Last Note Content","Waiting for Documents","Terminals (Y/N)",
            "Ecommerce (Y/N)","Offer - Started at","Offer - Completed at",
            "Customer Forms - Started at","Customer Forms - Completed at",
            "KYC Verification - Started at","KYC Verification - Completed at",
            "Underwriting - Started at","Underwriting - Completed at",
            "Finalisation - Started at","Finalisation - Completed at",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","INTEGER",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
        ]],
    },
    "Offer/Offer - Tags.csv": {
        "headers": [
            "Created Date","Offer ID","Package","Customer ID",
            "Customer Organisation Name","Sales Owner","Action Owner",
            "Company Name","Contact Name","Contact Email","Contact Number",
            "Customer Reference","Organisation Nickname","Individual Nickname",
            "Stage","Status","On Hold","Last Actioned By","Completed By",
            "Legal Entity Name","ABN","ACN","Registration Number",
            "UK Company Number","Avg Card Ticket Size","Annual Turnover",
            "Annual CC Turnover","Terminals QTY","Ecommerce (Y/N)",
            "Primary Partner","Billing Partner","Locale","Last Note Date",
            "Last Note Content","Waiting for Documents","Terminals (Y/N)",
            "Ecommerce (Y/N)","Offer - Started at","Offer - Completed at",
            "Customer Forms - Started at","Customer Forms - Completed at",
            "KYC Verification - Started at","KYC Verification - Completed at",
            "Underwriting - Started at","Underwriting - Completed at",
            "Finalisation - Started at","Finalisation - Completed at",
            "TAG-v4.54.3 system tag",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","INTEGER",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
            "ANY",
        ]],
    },
    "Offer/Offer - NoteCategories.csv": {
        "headers": [
            "Created Date","Offer ID","Package","Customer ID",
            "Customer Organisation Name","Sales Owner","Action Owner",
            "Company Name","Contact Name","Contact Email","Contact Number",
            "Customer Reference","Organisation Nickname","Individual Nickname",
            "Stage","Status","On Hold","Last Actioned By","Completed By",
            "Legal Entity Name","ABN","ACN","Registration Number",
            "UK Company Number","Avg Card Ticket Size","Annual Turnover",
            "Annual CC Turnover","Terminals QTY","Ecommerce (Y/N)",
            "Primary Partner","Billing Partner","Locale","Last Note Date",
            "Last Note Content","Waiting for Documents","Terminals (Y/N)",
            "Ecommerce (Y/N)","Offer - Started at","Offer - Completed at",
            "Customer Forms - Started at","Customer Forms - Completed at",
            "KYC Verification - Started at","KYC Verification - Completed at",
            "Underwriting - Started at","Underwriting - Completed at",
            "Finalisation - Started at","Finalisation - Completed at",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","INTEGER",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
        ]],
    },
    "Offer/Offer - TagsNoteCategories.csv": {
        "headers": [
            "Created Date","Offer ID","Package","Customer ID",
            "Customer Organisation Name","Sales Owner","Action Owner",
            "Company Name","Contact Name","Contact Email","Contact Number",
            "Customer Reference","Organisation Nickname","Individual Nickname",
            "Stage","Status","On Hold","Last Actioned By","Completed By",
            "Legal Entity Name","ABN","ACN","Registration Number",
            "UK Company Number","Avg Card Ticket Size","Annual Turnover",
            "Annual CC Turnover","Terminals QTY","Ecommerce (Y/N)",
            "Primary Partner","Billing Partner","Locale","Last Note Date",
            "Last Note Content","Waiting for Documents","Terminals (Y/N)",
            "Ecommerce (Y/N)","Offer - Started at","Offer - Completed at",
            "Customer Forms - Started at","Customer Forms - Completed at",
            "KYC Verification - Started at","KYC Verification - Completed at",
            "Underwriting - Started at","Underwriting - Completed at",
            "Finalisation - Started at","Finalisation - Completed at",
            "TAG-v4.54.3 system tag",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","INTEGER",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
            "ANY","ANY",
            "ANY",
        ]],
    },

    # ── Organisations ────────────────────────────────────────────────────
    "Organisations/Organisation Details - barebone.csv": {
        "headers": [
            "Created Date","Organisation ID","Organisation Name","Reference Code",
            "Partner Name","Trading As","Created By","Action Owner","Status",
            "Contact Phone","Contact Name","Contact Email","KYC Completed At",
            "Registered Country","Registration Number","ABN","ACN",
            "UK Company Number","Legal Entity Name","Entity Type","Verified",
            "OCDD Enabled","Has PEP/Sanctions?","Subscription Started At",
            "Last Renewal At","Next Renewal At","OCDD Last Run","OCDD Next Run",
            "Last OCDD Outcome","Relationship","OCDD Workflows","Risk Classes",
            "RECORD-Country?","RECORD-This is a test",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY",
        ]],
    },
    "Organisations/Organisation Details - Tags.csv": {
        "headers": [
            "Created Date","Organisation ID","Organisation Name","Reference Code",
            "Partner Name","Trading As","Created By","Action Owner","Status",
            "Contact Phone","Contact Name","Contact Email","KYC Completed At",
            "Registered Country","Registration Number","ABN","ACN",
            "UK Company Number","Legal Entity Name","Entity Type","Verified",
            "OCDD Enabled","Has PEP/Sanctions?","Subscription Started At",
            "Last Renewal At","Next Renewal At","OCDD Last Run","OCDD Next Run",
            "Last OCDD Outcome","Relationship","OCDD Workflows","Risk Classes",
            "RECORD-Country?","RECORD-This is a test",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY",
        ]],
    },

    # ── Partners ─────────────────────────────────────────────────────────
    "Partners/Partner Details - barebone.csv": {
        "headers": ["Partner ID","Partner Name","Contact Name","Phone","Partner Parent","Partner Type"],
        "rows": [["INTEGER","ANY","ANY","ANY","ANY","ANY"]],
    },

    # ── Prospects ────────────────────────────────────────────────────────
    "Prospects/Prospect barebone.csv": {
        "headers": [
            "Created Date","ID","Customer Name","Reference Code","Partner Name",
            "Trading As","Created By","Action Owner","Status","Contact Phone",
            "Contact Name","Contact Email","KYC Completed At","Registered Country",
            "Registration Number","ABN","ACN","UK Company Number","Legal Entity Name",
            "Entity Type","Verified","OCDD Enabled","Has PEP/Sanctions?",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Last OCDD Outcome","Relationship",
            "OCDD Workflows","Risk Classes","RECORD-Country?","RECORD-This is a test",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","Prospect",
            "ANY","ANY","ANY","ANY",
        ]],
    },
    "Prospects/Prospect Tags.csv": {
        "headers": [
            "Created Date","ID","Customer Name","Reference Code","Partner Name",
            "Trading As","Created By","Action Owner","Status","Contact Phone",
            "Contact Name","Contact Email","KYC Completed At","Registered Country",
            "Registration Number","ABN","ACN","UK Company Number","Legal Entity Name",
            "Entity Type","Verified","OCDD Enabled","Has PEP/Sanctions?",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Last OCDD Outcome","Relationship",
            "OCDD Workflows","Risk Classes","RECORD-Country?","RECORD-This is a test",
        ],
        "rows": [[
            "DATETIME","INTEGER","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","Prospect",
            "ANY","ANY","ANY","ANY",
        ]],
    },

    # ── Underwriting ActionOwners ────────────────────────────────────────
    "Underwriting ActionOwners/Customer_Prospect Details barebone.csv": {
        "headers": [
            "Created Date","ID","Customer Name","Reference Code","Partner Name",
            "Trading As","Created By","Action Owner","Status","Contact Phone",
            "Contact Name","Contact Email","KYC Completed At","Registered Country",
            "Registration Number","ABN","ACN","UK Company Number","Legal Entity Name",
            "Entity Type","Verified","OCDD Enabled","Has PEP/Sanctions?",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Last OCDD Outcome","Relationship",
            "OCDD Workflows","Risk Classes","RECORD-Annual Turnover",
            "RECORD-Can you see this?","RECORD-Country?","RECORD-Field 1",
        ],
        "rows": [[
            "DATETIME","INTEGER","MVSI PTY LTD","ANY","MVSI",
            "MVSI PTY LTD","ANY","ANY","Active","ANY",
            "ANY","ANY","DATETIME","Australia",
            "ANY","ANY","ANY","ANY","MVSI PTY LTD",
            "Australian Private Company","Yes","Yes","No",
            "DATETIME","DATETIME","DATETIME",
            "DATETIME","DATETIME","Pass","Customer",
            "Low Risk","Low Risk","ANY","ANY","ANY","Testing",
        ]],
    },
    "Underwriting ActionOwners/Customer_Prospect Details Tags.csv": {
        "headers": [
            "Created Date","ID","Customer Name","Reference Code","Partner Name",
            "Trading As","Created By","Action Owner","Status","Contact Phone",
            "Contact Name","Contact Email","KYC Completed At","Registered Country",
            "Registration Number","ABN","ACN","UK Company Number","Legal Entity Name",
            "Entity Type","Verified","OCDD Enabled","Has PEP/Sanctions?",
            "Subscription Started At","Last Renewal At","Next Renewal At",
            "OCDD Last Run","OCDD Next Run","Last OCDD Outcome","Relationship",
            "OCDD Workflows","Risk Classes","RECORD-Annual Turnover",
            "RECORD-Can you see this?","RECORD-Country?","RECORD-Field 1",
        ],
        "rows": [[
            "DATETIME","INTEGER","MVSI PTY LTD","ANY","MVSI",
            "MVSI PTY LTD","ANY","ANY","Active","ANY",
            "ANY","ANY","DATETIME","Australia",
            "ANY","ANY","ANY","ANY","MVSI PTY LTD",
            "Australian Private Company","Yes","Yes","No",
            "DATETIME","DATETIME","DATETIME",
            "DATETIME","DATETIME","Pass","Customer",
            "Low Risk","Low Risk","ANY","ANY","ANY","Testing",
        ]],
    },

    # ── Underwriting MyWork ──────────────────────────────────────────────
    "Underwriting MyWork/My Work Details barebone.csv": {
        "headers": [
            "ID","Customer Name","Record Type","Task","Status",
            "Completed Date","Earliest Follow-up","Created At",
        ],
        "rows": [[
            "INTEGER","ANY","ANY","ANY","ANY",
            "ANY","ANY","DATETIME",
        ]],
    },

    # ── Underwriting Underwriting ────────────────────────────────────────
    "Underwriting Underwriting/Approval Details barebone.csv": {
        "headers": [
            "Approval ID","Offer Name","Action Owner","Approval Name",
            "Approval Stage","Approval Condition","Status","Grouping",
            "Created At","Approved At","Approver - JoJo B","Approver - Wang Pin Lee",
        ],
        "rows": [[
            "INTEGER","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "DATETIME","ANY","ANY","ANY",
        ]],
    },

    # ── Users Groups (empty folder expected) ─────────────────────────────
    # No files expected – the folder should exist but be empty.

    # ── Users Users ──────────────────────────────────────────────────────
    "Users Users/User Details - barebone.csv": {
        "headers": ["Name","Role","Partner","Email Address","Phone","Created Date"],
        "rows": [["ANY","ANY","MVSI","ANY","ANY","DATETIME"]],
    },

    # ── Verifications ────────────────────────────────────────────────────
    "Verifications/Verification Details barebone.csv": {
        "headers": [
            "Instantiated Date","Created Date","Verification ID","Entity Name",
            "Verifier Name","Secondary Verifier Names","Partner Name","Offer Name",
            "Offer Reference Code","Verification Workflow","Closed At",
            "Person Name","Closed Reason","Closed By","Locked By","Status",
            "Outcome","Action Owner","Offer On Hold","Reverification",
            "Billing Partner","Verification Type","Waiting for Documents",
            "Last Note Date","Last Note Content",
        ],
        "rows": [[
            "DATETIME","DATETIME","INTEGER","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY",
        ]],
    },
    "Verifications/Verification Details Entity Type Counters.csv": {
        "headers": [
            "Instantiated Date","Created Date","Verification ID","Entity Name",
            "Verifier Name","Secondary Verifier Names","Partner Name","Offer Name",
            "Offer Reference Code","Verification Workflow","Closed At",
            "Person Name","Closed Reason","Closed By","Locked By","Status",
            "Outcome","Action Owner","Offer On Hold","Reverification",
            "Billing Partner","Verification Type","Waiting for Documents",
            "Last Note Date","Last Note Content",
        ],
        "rows": [[
            "DATETIME","DATETIME","INTEGER","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY",
        ]],
    },
    "Verifications/Verification Details Note Categories.csv": {
        "headers": [
            "Instantiated Date","Created Date","Verification ID","Entity Name",
            "Verifier Name","Secondary Verifier Names","Partner Name","Offer Name",
            "Offer Reference Code","Verification Workflow","Closed At",
            "Person Name","Closed Reason","Closed By","Locked By","Status",
            "Outcome","Action Owner","Offer On Hold","Reverification",
            "Billing Partner","Verification Type","Waiting for Documents",
            "Last Note Date","Last Note Content",
        ],
        "rows": [[
            "DATETIME","DATETIME","INTEGER","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY",
        ]],
    },
    "Verifications/Verification Details Tags.csv": {
        "headers": [
            "Instantiated Date","Created Date","Verification ID","Entity Name",
            "Verifier Name","Secondary Verifier Names","Partner Name","Offer Name",
            "Offer Reference Code","Verification Workflow","Closed At",
            "Person Name","Closed Reason","Closed By","Locked By","Status",
            "Outcome","Action Owner","Offer On Hold","Reverification",
            "Billing Partner","Verification Type","Waiting for Documents",
            "Last Note Date","Last Note Content",
        ],
        "rows": [[
            "DATETIME","DATETIME","INTEGER","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY","ANY",
            "ANY","ANY","ANY",
            "ANY","ANY",
        ]],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Result data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FileResult:
    """Verification result for a single file."""
    relative_path: str
    status: str = "PASS"                    # PASS | FAIL | MISSING | UNEXPECTED
    header_ok: bool = True
    header_details: str = ""
    row_count_ok: bool = True
    row_count_details: str = ""
    cell_issues: list = field(default_factory=list)   # list of strings
    placeholder_issues: list = field(default_factory=list)  # list of strings

    @property
    def passed(self) -> bool:
        return self.status == "PASS"


# ─────────────────────────────────────────────────────────────────────────────
# Core verification logic
# ─────────────────────────────────────────────────────────────────────────────

def _read_csv(filepath: Path) -> tuple[list[str], list[list[str]]]:
    """Read a CSV and return (headers, data_rows)."""
    with open(filepath, "r", newline="", encoding="utf-8-sig") as fh:
        text = fh.read()
    # Normalise line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return [], []
    headers = [h.strip() for h in rows[0]]
    data = rows[1:]
    return headers, data


def verify_file(base_dir: Path, rel_path: str, ref: dict) -> FileResult:
    """Verify one downloaded file against its reference definition."""
    result = FileResult(relative_path=rel_path)
    filepath = base_dir / rel_path

    if not filepath.exists():
        result.status = "MISSING"
        return result

    headers, data_rows = _read_csv(filepath)

    # ── Header check ─────────────────────────────────────────────────────
    expected_headers = ref["headers"]
    if headers != expected_headers:
        result.header_ok = False
        result.status = "FAIL"
        missing_h = [h for h in expected_headers if h not in headers]
        extra_h = [h for h in headers if h not in expected_headers]
        parts = []
        if missing_h:
            parts.append(f"missing columns: {missing_h}")
        if extra_h:
            parts.append(f"extra columns: {extra_h}")
        if not missing_h and not extra_h:
            parts.append("column order differs")
        result.header_details = "; ".join(parts)

    # ── Row-count check ──────────────────────────────────────────────────
    expected_count = ref.get("expected_row_count")
    min_count = ref.get("min_row_count")
    if expected_count is not None and len(data_rows) != expected_count:
        result.row_count_ok = False
        result.status = "FAIL"
        result.row_count_details = (
            f"expected {expected_count} data rows, got {len(data_rows)}"
        )
    if min_count is not None and len(data_rows) < min_count:
        result.row_count_ok = False
        result.status = "FAIL"
        result.row_count_details = (
            f"expected at least {min_count} data rows, got {len(data_rows)}"
        )

    # ── Cell-level checks ────────────────────────────────────────────────
    row_patterns = ref.get("rows")
    if row_patterns == "ANY_ROWS" or row_patterns is None:
        return result  # nothing more to check

    for row_idx, expected_row in enumerate(row_patterns):
        if row_idx >= len(data_rows):
            result.cell_issues.append(f"Row {row_idx+1}: row missing from file")
            result.status = "FAIL"
            continue
        actual_row = data_rows[row_idx]
        for col_idx, pattern_token in enumerate(expected_row):
            col_name = expected_headers[col_idx] if col_idx < len(expected_headers) else f"col_{col_idx}"
            actual_val = actual_row[col_idx].strip() if col_idx < len(actual_row) else ""
            pat = _compile_pattern(pattern_token)
            if not pat.match(actual_val):
                result.cell_issues.append(
                    f"Row {row_idx+1}, [{col_name}]: "
                    f"expected pattern '{pattern_token}' but got '{actual_val[:80]}'"
                )
                result.status = "FAIL"

    return result


def scan_placeholders(base_dir: Path, results: list[FileResult]) -> int:
    """Scan every CSV file for placeholder / pseudo-blank values.

    Mutates each FileResult in *results* by appending to its
    ``placeholder_issues`` list.  Returns the total count of issues found.
    """
    total = 0
    # Build a lookup by relative path for quick access
    result_map: dict[str, FileResult] = {r.relative_path: r for r in results}

    for root, _dirs, files in os.walk(base_dir):
        for f in files:
            if not f.lower().endswith(".csv"):
                continue
            filepath = Path(root) / f
            rel = str(filepath.relative_to(base_dir))
            headers, data_rows = _read_csv(filepath)
            for row_idx, row in enumerate(data_rows):
                for col_idx, raw_val in enumerate(row):
                    col_name = headers[col_idx] if col_idx < len(headers) else f"col_{col_idx}"
                    for pattern, reason in PLACEHOLDER_PATTERNS:
                        if pattern.search(raw_val):
                            msg = (
                                f"Row {row_idx+1}, [{col_name}]: "
                                f"\"{raw_val.strip()[:60]}\" — {reason}"
                            )
                            if rel in result_map:
                                result_map[rel].placeholder_issues.append(msg)
                            else:
                                # File is unexpected; create a transient result
                                fr = FileResult(relative_path=rel, status="UNEXPECTED")
                                fr.placeholder_issues.append(msg)
                                results.append(fr)
                                result_map[rel] = fr
                            total += 1
                            break  # one match per cell is enough
    return total


def discover_actual_files(base_dir: Path) -> set[str]:
    """Walk the base_dir and return all CSV relative paths."""
    found = set()
    for root, _dirs, files in os.walk(base_dir):
        for f in files:
            if f.lower().endswith(".csv"):
                full = Path(root) / f
                found.add(str(full.relative_to(base_dir)))
    return found


# ─────────────────────────────────────────────────────────────────────────────
# Pretty-print summary
# ─────────────────────────────────────────────────────────────────────────────

_BOLD   = "\033[1m"
_RED    = "\033[91m"
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_CYAN   = "\033[96m"
_RESET  = "\033[0m"


def _colour(status: str) -> str:
    mapping = {"PASS": _GREEN, "FAIL": _RED, "MISSING": _YELLOW, "UNEXPECTED": _CYAN}
    return f"{mapping.get(status, '')}{status}{_RESET}"


def print_summary(results: list[FileResult], unexpected: list[str],
                  placeholder_count: int):
    """Print a human-readable summary table."""
    _MAGENTA = "\033[95m"

    print()
    print(f"{_BOLD}{'=' * 100}{_RESET}")
    print(f"{_BOLD}  EXPORTED FILE VERIFICATION SUMMARY{_RESET}")
    print(f"{_BOLD}{'=' * 100}{_RESET}")
    print()

    # ── Summary counts ───────────────────────────────────────────────────
    passed  = sum(1 for r in results if r.status == "PASS")
    failed  = sum(1 for r in results if r.status == "FAIL")
    missing = sum(1 for r in results if r.status == "MISSING")
    extra   = len(unexpected)
    files_with_placeholders = sum(1 for r in results if r.placeholder_issues)

    print(f"  Total expected files : {len(results)}")
    print(f"  {_GREEN}✓ Passed{_RESET}             : {passed}")
    print(f"  {_RED}✗ Failed{_RESET}             : {failed}")
    print(f"  {_YELLOW}⚠ Missing{_RESET}            : {missing}")
    print(f"  {_CYAN}? Unexpected{_RESET}         : {extra}")
    print(f"  {_MAGENTA}⊘ Placeholders{_RESET}       : {placeholder_count} value(s) across {files_with_placeholders} file(s)")
    print()

    # ── File-by-file table ───────────────────────────────────────────────
    col_w = max((len(r.relative_path) for r in results), default=40)
    col_w = max(col_w, max((len(u) for u in unexpected), default=0))
    col_w = max(col_w, 40)

    header_line = f"  {'File':<{col_w}}  {'Status':<12}  Details"
    print(f"{_BOLD}{header_line}{_RESET}")
    print(f"  {'─' * col_w}  {'─' * 12}  {'─' * 40}")

    for r in sorted(results, key=lambda x: (x.status != "MISSING", x.status != "FAIL", x.relative_path)):
        details_parts = []
        if not r.header_ok:
            details_parts.append(f"Headers: {r.header_details}")
        if not r.row_count_ok:
            details_parts.append(r.row_count_details)
        if r.cell_issues:
            details_parts.append(f"{len(r.cell_issues)} cell issue(s)")
        if r.placeholder_issues:
            details_parts.append(f"{_MAGENTA}{len(r.placeholder_issues)} placeholder(s){_RESET}")
        detail_str = "; ".join(details_parts) if details_parts else ""
        print(f"  {r.relative_path:<{col_w}}  {_colour(r.status):<22}  {detail_str}")

    for u in sorted(unexpected):
        print(f"  {u:<{col_w}}  {_colour('UNEXPECTED'):<22}  File not in reference set")

    print()

    # ── Detailed cell issues ─────────────────────────────────────────────
    any_issues = any(r.cell_issues for r in results)
    if any_issues:
        print(f"{_BOLD}{'─' * 100}{_RESET}")
        print(f"{_BOLD}  CELL-LEVEL ISSUES{_RESET}")
        print(f"{_BOLD}{'─' * 100}{_RESET}")
        for r in results:
            if r.cell_issues:
                print(f"\n  {_BOLD}{r.relative_path}{_RESET}")
                for issue in r.cell_issues:
                    print(f"    • {issue}")
        print()

    # ── Placeholder / pseudo-blank issues ────────────────────────────────
    if placeholder_count > 0:
        print(f"{_BOLD}{'─' * 100}{_RESET}")
        print(f"{_BOLD}{_MAGENTA}  PLACEHOLDER / PSEUDO-BLANK VALUES{_RESET}")
        print(f"{_BOLD}{'─' * 100}{_RESET}")
        print(f"  Values that are not real data — serialisation artefacts,")
        print(f"  whitespace masquerading as blank, or programmatic nulls.")
        for r in results:
            if r.placeholder_issues:
                print(f"\n  {_BOLD}{r.relative_path}{_RESET}")
                for issue in r.placeholder_issues:
                    print(f"    ⊘ {issue}")
        print()

    # ── Final verdict ────────────────────────────────────────────────────
    print(f"{'=' * 100}")
    if failed == 0 and missing == 0 and extra == 0 and placeholder_count == 0:
        print(f"  {_GREEN}{_BOLD}ALL CHECKS PASSED ✓{_RESET}")
    else:
        parts = []
        if failed > 0 or missing > 0 or extra > 0:
            parts.append(f"{_RED}{_BOLD}STRUCTURAL/CONTENT CHECKS FAILED{_RESET}")
        if placeholder_count > 0:
            parts.append(f"{_MAGENTA}{_BOLD}{placeholder_count} PLACEHOLDER VALUE(S) DETECTED{_RESET}")
        print(f"  {' | '.join(parts)}")
    print(f"{'=' * 100}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) > 1:
        base_dir = Path(sys.argv[1])
    else:
        base_dir = Path(__file__).parent / "downloaded exported files"

    if not base_dir.is_dir():
        print(f"ERROR: directory not found: {base_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"\nScanning: {base_dir.resolve()}\n")

    # 1. Verify each expected file
    results: list[FileResult] = []
    for rel_path, ref in REFERENCE_FILES.items():
        result = verify_file(base_dir, rel_path, ref)
        results.append(result)

    # 2. Detect unexpected files
    expected_set = set(REFERENCE_FILES.keys())
    actual_set = discover_actual_files(base_dir)
    unexpected = sorted(actual_set - expected_set)

    # 3. Scan for placeholder / pseudo-blank values
    placeholder_count = scan_placeholders(base_dir, results)

    # 4. Print summary
    print_summary(results, unexpected, placeholder_count)

    # Exit with non-zero if anything failed
    has_issues = (
        any(not r.passed for r in results)
        or len(unexpected) > 0
        or placeholder_count > 0
    )
    sys.exit(1 if has_issues else 0)


if __name__ == "__main__":
    main()
