Steps for creating a new Java repository in Reaktivity.

1. Name the new repository `[repository-name].java` with README only
2. Create a new branch called `develop`
3. Make the `develop` branch the default branch
4. Protect the `develop` branch (check everything except "Include Administrators")
5. Protect the `master` branch (check only "Protect this branch")
6. Clone the new repository locally, then
```
$ git config merge.ours.driver true
$ git remote add --track develop build https://github.com/reaktivity/build-template.java
$ git fetch build develop
$ git merge build/develop --no-commit
```
... review the changes, modify the `pom.xml` to reflect your project name, description, and artifact id before committing the changes.
```
$ git commit
```
