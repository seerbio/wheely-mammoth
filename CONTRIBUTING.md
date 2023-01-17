## Contributing to `wheely-mammoth`

### Setting up for development

1. Clone the repository

    ```shell
    git clone git@github.com:seerbio/wheely-mammoth.git
    cd wheely-mammoth
    ```

    Alternatively, create a fork through GitHub and clone that repository.

2. Set up `pre-commit` hook (for code formatting):

    ```shell
    pip install pre-commit
    pre-commit install
    ```

    This will ensure all Python sources are consistently formatted whenever
    you commit to the repository.

### Developing changes

1. Create a branch

    ```shell
    git checkout main
    git pull --ff-only --tags origin main
    git checkout -b my-feature-branch main
    ```

2. Develop your changes. Commit often, with meaningful messages for each commit.
   Be sure to develop unit tests and ensure that all existing tests pass:

    ```shell
    pytest
    ```

3. Push changes back to the repository (or your fork).
4. Create a pull request through GitHub. Assign yourself to it and add appropriate
   reviewers.

### Releasing `wheely-mammoth`

After changes are developed, reviewed, and merged, it's possible to create a
release automatically using GitHub.

1. Determine the appropriate [semantic version](https://semver.org/) for the
   new release. Check the GitHub repo's "Releases" section to see what the most
   recent release number is and consider the changes made.
2. Create a Release through the GitHub UI. Choose to create a new tag targeting
   the `main` branch and name it for the new version number, i.e. `v3.4.5`.
   Use the same string as the release title. Click "generate release notes" to
   automatically create a list of PRs since the last release.
3. After creating the release, the package will be automatically built and
   deployed with the new version number.