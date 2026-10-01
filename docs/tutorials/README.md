# istSOS4 Tutorial

This folder contains the python notebooks used in the tutorials.

# Git Workflow: Merging Branches

## Purpose

This guide explains how to merge the changes from the `traveltime` branch into the `traveltime_edu` branch.

## Steps

1. **Make sure you are on the `traveltime_edu` branch:**

   ```bash
   git checkout traveltime_edu
   ```

2. **Pull the latest changes of `traveltime`:**

   Before merging, make sure the `traveltime` branch is up to date:

   ```bash
   git checkout traveltime
   git pull origin traveltime
   ```

3. **Go back to `traveltime_edu`:**

   ```bash
   git checkout traveltime_edu
   ```

4. **Merge the changes from `traveltime` into `traveltime_edu`:**

   ```bash
   git merge traveltime
   ```

   If there are conflicts, Git tells you and you have to resolve them by hand. After resolving them, save the modified files and commit:

   ```bash
   git add .
   git commit
   ```

5. **Push the changes (optional, if you work with a remote repository):**

   ```bash
   git push origin traveltime_edu
   ```

This way all the changes from the `traveltime` branch are merged into `traveltime_edu`.
