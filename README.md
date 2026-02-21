# Exported File Verifier

[![Python](https://img.shields.io/badge/Python-3.14+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Node.js](https://img.shields.io/badge/Node.js-18+-339933?style=flat-square&logo=node.js&logoColor=white)](https://nodejs.org/)
[![Commitlint](https://img.shields.io/badge/commitlint-20.4.2-000000?style=flat-square&logo=commitlint&logoColor=white)](https://commitlint.js.org/)
[![Husky](https://img.shields.io/badge/husky-9.1.7-42b983?style=flat-square&logo=git&logoColor=white)](https://typicode.github.io/husky/)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue?style=flat-square)](https://opensource.org/licenses/ISC)

A Python tool that compares downloaded CSV export files against a known set of reference definitions — validating **file presence**, **column headers**, **cell-level content**, and flagging **placeholder / pseudo-blank values**.

---

## Features

- **File Presence Check** — detects missing files and unexpected files not in the reference set
- **Header Validation** — verifies each CSV has the exact expected column headers in the correct order
- **Cell Content Validation** — compares each cell against expected values using exact match or regex patterns for dynamic data (dates, IDs, etc.)
- **Placeholder Detection** — flags non-real data such as:
  - `[object Object]` (JavaScript serialisation bugs)
  - Whitespace-only strings masquerading as blank
  - Programmatic nulls (`null`, `undefined`, `NaN`, `None`)
  - Spreadsheet error values (`#N/A`, `#REF!`, `#VALUE!`, `#DIV/0!`)
- **Colour-coded Summary Table** — easy-to-read terminal output with pass/fail/missing/unexpected/placeholder counts

## Dynamic Data Patterns

For columns with values that change between runs, the following regex tokens are used instead of exact strings:

| Token | Matches | Example |
|-------|---------|---------|
| `DATETIME` | `dd-Mon-yyyy HH:MM:SS` | `19-Feb-2026 11:55:33` |
| `DATE_SLASH` | `dd/mm/yyyy` | `24/02/2000` |
| `INTEGER` | One or more digits | `255529` |
| `ANY` | Any string (including empty) | — |
| `EMPTY` | Must be empty | — |
| `NONEMPTY` | At least one character | — |

## Usage

### Run the verifier

```bash
# Against the default ./downloaded exported files directory
python3 verify_exports.py

# Against a specific directory
python3 verify_exports.py /path/to/new/batch

# Or via npm
npm run verify
```

### Sample Output

```
====================================================================================================
  EXPORTED FILE VERIFICATION SUMMARY
====================================================================================================

  Total expected files : 40
  ✓ Passed             : 40
  ✗ Failed             : 0
  ⚠ Missing            : 0
  ? Unexpected         : 0
  ⊘ Placeholders       : 202 value(s) across 8 file(s)
```

## Project Structure

```
.
├── README.md
├── verify_exports.py                  # Main verification script
├── downloaded exported files/         # Reference CSV files (40 files across 25 folders)
│   ├── Customer/
│   ├── Individuals/
│   ├── Offer/
│   ├── Verifications/
│   └── ...
├── .gitignore
├── .husky/                            # Git hooks
│   ├── commit-msg                     # Commitlint validation
│   ├── pre-commit                     # (no-op)
│   └── pre-push                       # Branch naming check
├── commitlint.config.js
├── package.json
└── package-lock.json
```

## Modules & Dependencies

### Python (standard library only — no pip install required)

| Module | Version | Description |
|--------|---------|-------------|
| `csv` | stdlib | CSV file reading and parsing |
| `re` | stdlib | Regex pattern matching for dynamic data |
| `io` | stdlib | In-memory string streams for CSV processing |
| `os` | stdlib | File system traversal |
| `sys` | stdlib | CLI argument parsing and exit codes |
| `pathlib` | stdlib | Cross-platform path handling |
| `dataclasses` | stdlib | Structured result objects |

### Node.js (dev dependencies — for Git hooks only)

| Module | Version | Description |
|--------|---------|-------------|
| [![@commitlint/cli](https://img.shields.io/npm/v/@commitlint/cli?style=flat-square&label=%40commitlint%2Fcli&color=000)](https://www.npmjs.com/package/@commitlint/cli) | `20.4.2` | Lint commit messages against conventional commit format |
| [![@commitlint/config-conventional](https://img.shields.io/npm/v/@commitlint/config-conventional?style=flat-square&label=%40commitlint%2Fconfig-conventional&color=000)](https://www.npmjs.com/package/@commitlint/config-conventional) | `20.4.2` | Shareable commitlint config for conventional commits |
| [![branch-naming-check](https://img.shields.io/npm/v/branch-naming-check?style=flat-square&label=branch-naming-check&color=blue)](https://www.npmjs.com/package/branch-naming-check) | `1.0.2` | Validates Git branch names against a regex pattern |
| [![husky](https://img.shields.io/npm/v/husky?style=flat-square&label=husky&color=42b983)](https://www.npmjs.com/package/husky) | `9.1.7` | Modern Git hooks manager |

## Git Conventions

### Commit Messages

Enforced by [commitlint](https://commitlint.js.org/) using the [Conventional Commits](https://www.conventionalcommits.org/) standard:

```
<type>: <description>

# Examples:
feat: add new CSV reference for Partners module
fix: correct datetime regex to handle single-digit days
chore: update dependencies
docs: update README with usage examples
```

Allowed types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`

### Branch Naming

Enforced on `git push` via the `pre-push` hook:

```
feature/*    — new features
bugfix/*     — bug fixes
hotfix/*     — urgent production fixes
release/*    — release preparation
main         — production branch
develop      — development branch
```

## Setup

```bash
# Clone the repo
git clone git@github.com:yuzhangoscar/exportedFileVerifier.git
cd exportedFileVerifier

# Install Node.js dev dependencies (for Git hooks)
npm install
```

No Python packages need to be installed — the script uses only the standard library.
