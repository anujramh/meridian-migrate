# Contributing to Meridian

Thank you for your interest in contributing! Meridian is fully open source and community driven.

## Ways to contribute

- **Add a cloud scanner** — GCP, Azure, and more needed
- **Add migration playbooks** — document real-world migration paths
- **Improve error handling** — make failures clearer and more actionable
- **Write tests** — help us build a reliable test suite
- **Fix bugs** — check open issues
- **Improve docs** — make it easier for new users to get started

## Getting started
```bash
git clone https://github.com/anujramh/meridian-migrate.git
cd meridian-migrate
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

## Project structure
```
meridian/
├── cli.py              # CLI commands
├── scanners/
│   ├── aws.py          # AWS scanner
│   ├── oracle.py       # Oracle Cloud scanner
│   └── gcp.py          # GCP scanner (coming soon)
```

## Adding a new cloud scanner

1. Create `meridian/scanners/<cloud>.py`
2. Implement `scan(mock=False, ...)` function
3. Add mock data for testing without credentials
4. Add CLI command in `meridian/cli.py`
5. Update the supported clouds table in `README.md`

## Submitting a pull request

1. Fork the repo
2. Create a branch: `git checkout -b feat/your-feature`
3. Make your changes
4. Test with mock mode: `meridian scan-aws --mock`
5. Push and open a PR

## Code style

- Python 3.8+
- Follow existing patterns in `meridian/scanners/`
- Always add mock mode for new scanners
- Handle errors gracefully — never crash, always give a helpful message

## Questions?

Open a GitHub Discussion or file an issue.
