# Contributing

This repository is generated from Wakeshift's main repository. Pull requests are welcome: we apply them there and publish the result.

We read every issue. Report bugs, ask questions and suggest changes in this repository's issues.

Pull requests are welcome; we don't guarantee we'll accept every one. An accepted pull request is applied to Wakeshift's main repository, keeping you as the commit's author, and arrives here with the next publish; we close the pull request here with a link to it.

Before you open one:

- Run `uv run ruff check`, `uv run pyright` and `uv run pytest`.
- Keep the change focused, and say what it fixes or adds.
- Don't include keys, tokens or other secrets, including in tests.
- Change only the code: files this repository's export generates (`.source-commit`) are updated when it's published.

Report a security problem privately to support@wakeshift.ai, not in an issue.

By contributing, you agree your contribution is licensed under the Apache License 2.0.
