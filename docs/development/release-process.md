---
title: Release Process
---

# Release Process

This document outlines the steps required to create and publish a new release of Headendarr.

## 1. Prepare the Environment

Ensure you are working on the latest code from the `master` branch.

```bash
git checkout master
git pull origin master
```

## 2. Validation and Testing

Before cutting a release, it is critical to verify the stability and upgrade path of the application.

### Build Verification

Ensure the project builds correctly without errors.

```bash
task frontend:build
task backend:build
```

### Clean Install Verification

Verify that the application can start from scratch with a fresh database and configuration directory.

1.  Stop any running instances.
2.  Temporarily move or rename your existing `./dev_env/config` directory.
3.  Start the application: `task all:start`
4.  Access the UI and complete the initial setup.
5.  Verify that all core features function as expected.

### Migration Verification

Verify that the application can successfully migrate data from the previous release.

1.  Start with a configuration directory/database from the _previous_ stable release.
2.  Start the new version of Headendarr.
3.  Monitor logs (`task backend:logs`) to ensure Alembic migrations complete without errors.
4.  Verify that existing settings, sources, and channel mappings are preserved and functional.

## 3. Version Bump and Publication

Once validation is complete, you can trigger the automated release pipeline.

### Set the New Version

Use the Taskfile command to update the version in `frontend/package.json` and `frontend/package-lock.json`.

```bash

task set-version -- 1.0.1

```

### Commit and Push

Commit the version bump and push directly to the `master` branch.

```bash

git add frontend/package.json frontend/package-lock.json

git commit -m "Bump version to 1.0.1"

git push origin master

```

## 4. Automated Release Pipeline

Pushing to the `master` branch triggers the GitHub Actions CI/CD pipeline, which will:

1.  Detect the new version in `frontend/package.json`.
2.  Verify that no Git tag for this version already exists.
3.  Build and push versioned Docker images to Docker Hub and GHCR.
4.  Automatically create and push a new Git tag (e.g., `v1.0.1`).
