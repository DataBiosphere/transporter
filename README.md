# Transporter
Bulk file-transfer system for data ingest / delivery.

## Why Transporter?
Data ingest into our cloud environment requires bulk file transfer from both on-prem storage and other clouds.
Similarly, data delivery requires uploading large files to external systems. Transporter aims to provide uniform
(but extensible) APIs for submitting, monitoring, and executing bulk transfers between these various environments.

## Building
Transporter is built using `sbt`. Installation instructions for `sbt` are [here](https://www.scala-sbt.org/download.html).

Once you're set up, drop into the REPL by running `sbt` from the project root. From there, you can `compile`, `test`, etc:
```bash
$ sbt
... log output ...
sbt:transporter> compile
```

It's also possible to run individual `sbt` commands directly from bash. This is _not_ the recommended way to use `sbt`,
as you'll eat the tool's (nontrivial) startup costs on every command.

## Developing
Two main options exist for Scala development:
  1. IntelliJ + the Scala plugin is well-established, and provides the best support for refactoring. On the negative side,
     it uses a reverse-engineered version of the scalac presentation compiler which sometimes marks valid code with errors.
  2. [Metals](https://scalameta.org/metals/) (primarily combined with VS Code) is the new hotness. It has fewer features
     than IntelliJ, but it delegates all build/compile/test to the underlying build tool so its reporting is usually more accurate.

In either tool, opening the root `transporter` directory should bring up an "import build" dialog.
