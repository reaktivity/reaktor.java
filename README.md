## Creating a new Java repository in Reaktivity

1. Name the new repository `[repository-name].java` with `README` only
2. Enable issues only (no wiki or restricted wiki edits)
3. Create a new branch called `develop`
4. Make the `develop` branch the default branch
5. Protect the `develop` branch (check everything except "Include Administrators")
6. Protect the `master` branch (check only "Protect this branch")
7. Clone the new repository locally, then
```bash
$ git config merge.ours.driver true
$ git remote add --track develop build https://github.com/reaktivity/build-template.java
$ git fetch build develop
$ git merge build/develop --allow-unrelated-histories --no-commit
```
Review the changes, modify the `pom.xml` to reflect your project `name`, `description`, and `artifactId`.
Activate the new repository in Travis CI.
Commit the changes and push them back.
```bash
$ git add pom.xml
$ git commit
$ git push origin develop
```
Verify the Travis CI build success, then further protect the `develop` branch by requiring the `continuous-integration/travis-ci` status check.

Slack Travis CI application configuration gives token string of the form `reaktivity:XXXXXXXXXXXXXXXXXXXXXXXX`.
```bash
$ travis encrypt "reaktivity:XXXXXXXXXXXXXXXXXXXXXXXX" --add notifications.slack
$ git add .travis.yml
$ git commit -m "Notify build results via Slack"
$ git push origin develop
```
Verify the Travis CI build notification is delivered via Slack.
