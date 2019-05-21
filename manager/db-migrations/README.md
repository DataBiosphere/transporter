# Transporter Manager - DB Migrations
Liquibase migrations for the Manager service.

## Why this Structure?
We separate our Liquibase migrations into a distinct sub-project so we can package
them into an image that can run as an init container in GKE. This has upsides and
downsides vs. packaging them into main app code as a startup step (more fine-grained
control over when migrations run, at the cost of having another moving piece).

## Running Migrations Locally
We've overridden the default `run` command in `sbt` for this sub-project to migrate
a locally-running Postgresql DB. We assume:
1. The local machine is running OS X
2. Postgres has been installed using Homebrew
3. Postgres is running on the default port (5432)
4. The database to migrate is named "postgres" (Homebrew default)
5. The username to log into the DB with matches the OS X username of the caller (Homebrew default)
6. There is no password for the DB user (Homebrew default)

If all of the above are true, this should work:
```bash
$ sbt
sbt:transporter> transporter-manager-migrations/run
... liquibase output ...
```

## Running Migrations on Deploy
See the [transporter-deploy](github.com/broadinstitute/transporter-deploy/README.md) repository
for an example of how migrations can run as an init container through the Cloudsql proxy.
