# migrate

`migrate` is a database migration tool that currently works across MySQL,
Postgres, and sqlite3.

`migrate` ensures your database reaches consistent state in any environment.
Unlike most database migration tools, `migrate` enforces two key concepts:

* Migrations must be the same every run
* Migrations must not be inserted earlier in history

If either of these concepts cannot be satisifed, that means consistent state
cannot be reached, and `migrate` fails loudly.

`migrate` enforces these two concepts by recording the history of completed
migrations in the database itself under a `meta` table. It checks that table
every run to ensure that no migration was inserted earlier in history and that
no already-run migration file has changed via its checksum.

Other tools for database migrations introduced the concept of "up" and "down"
migrations. There are several drawbacks to that approach, but the biggest by
far is that "down" migrations may incorrectly reverse an "up" migration due to
programmer error, leaving the database in an inconsistent state across
environments.

Thus, "down" migrations compromise the entire point of any migration tool,
which is above all else to guarantee consistent state. At the same time, it's
possible to write every "down" migration as an "up" migration: simply write the
migration to be performed as another explicit step in the database's history.

Thus, `migrate` eliminates the concept of directional migrations altogether. With
`migrate`, every migration is an "up" migration -- every migration moves you
forward in history, and consistency can always be reached.

## Install

```
go get github.com/egtann/migrate/cmd/migrate
```

## Usage

```
$ ls db/migrations
1_create_users.sql
2_create_messages.sql
3_add_user_id_to_messages.sql

$ migrate -db my_database -dir db/migrations
```

Each migration must be a plain SQL file that ends with `.sql`.

**Note on numbering:** To enforce that no migration is inserted earlier in
history, `migrate` requires that migration filenames start with ordered
numbers. This can be `1`, `2`, `3` as above, or it can be a UNIX timestamp or
even a formatted timestamp like `YYYYMMDD##`, such as `2018060101`.

Run `migrate -h` for available flags.

## How to use migrate with an existing database

First, ensure that all your migration filenames are numbered as described
above. Then run `migrate` with the `-skip` flag. For example, if a project had
70 migrations that had already been run and another several migrations which
had not (that is, you want to migrate starting at #71), you'd run:

```
migrate -db my_database -dir db/migrations -skip 71_the_last_migration_you_ran.sql
```

Adding `-skip` will populate the `meta` table with the history to that point,
and then run all migrations beyond that point. You only need to pass the
`-skip` flag one time per database.

## Known limitations

The following features are not available yet but will be added:

* **Comments:** Currently there's minimal support for comments in the migration
  files. Comments must be at the start of lines.
