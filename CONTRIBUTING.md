# Contributing to DBT Server

First off, thank you for considering contributing to DBT Server. It's people like you that make DBT Server such a great tool.

## Getting Started

- Make sure you have a [GitHub account](https://github.com/signup/free)
- Submit a ticket for your issue, assuming one does not already exist.
  - Clearly describe the issue including steps to reproduce when it is a bug.
  - Make sure you fill in the earliest version that you know has the issue.

## Making Changes

- Fork the repository on GitHub.
- Create a topic branch from where you want to base your work.
  - This is usually the main branch.
  - Only target release branches if you are certain your fix must be on that branch.
  - To quickly create a topic branch based on main; `git branch fix/main/my_contribution main`. Then checkout the new branch with `git checkout fix/main/my_contribution`.
- Make commits of logical units.
- Check for unnecessary whitespace with `git diff --check` before committing.
- Make sure your commit messages are in the [proper format](https://www.conventionalcommits.org/en/v1.0.0/).

## Submitting Changes

- Push your changes to a topic branch in your fork of the repository.
- Submit a pull request to the repository in the artefactory organization.
- The core team looks at Pull Requests on a regular basis.
- After feedback has been given we expect responses within two weeks. After two weeks we may close the PR if it isn't showing any activity.

## Code Style

Follow the existing code style.
- isort
- pyupgrade
- black
- mypy


## License

By contributing your code, you agree to license your contribution under the terms of the MIT: https://choosealicense.com/licenses/mit/

## All contributions to DBT Server will be released under the MIT license. By submitting a pull request, you are agreeing to comply with this license and for any and all legal responsibility for your contributions.
