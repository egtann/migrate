# Migrate

Migrate is a database migration tool for MySQL. It's a new take on database
migrations, designed to satisfy the needs of real projects without the
drawbacks of other designs.

## Why

Migrate ensures your database reaches consistent state in any environment.
Unlike most database migration tools, Migrate enforces two key concepts:

* Migrations must be the same every run
* Migrations must not be inserted earlier in history

If either of these concepts cannot be satisifed, that means consistent state
cannot be reached, and Migrate fails loudly.

Migrate enforces these two concepts by recording the history of completed
migrations in the database itself under a `meta` table. It checks that table
every run to ensure that no migration was inserted earlier in history and that
no already-run migration file has changed via its checksum.

Migrate also takes a different approach to what it considers a migration. Most
of the time to migrate the database you only need SQL, and Migrate allows you
to use standard SQL files. However when business requirements change, a
database may need to restructure its data without losing any, and SQL may not
be the best or easiest tool for the job. It's critical to include
transformation steps like these in the migration history to achieve consistent
state, so Migrate allows you to use any program to perform migrations, such as
go, python, node, or ruby. Simply add a `#!` (shebang) instruction to the top
of the file to execute. The one exception is any `.go` file, which should not
include a shebang instruction.

Other tools for database migrations introduced the concept of "up" and "down"
migrations. There are several drawbacks to that approach, but the biggest by
far is the following:

   "Down" migrations may incorrectly reverse an "up" migration due to
   programmer error, leaving the database in an inconsistent state across
   environments.

Thus, down migrations can compromise the entire point of any migration tool,
which is to achieve consistent state. At the same time, it's possible to write
every "down" migration as an "up" migration: simply write the migration to be
performed as another explicit step in the database's history.

Thus, Migrate eliminates the concept of directional migrations altogether. With
Migrate, every migration is an "up" migration -- every migration moves you
forward in history, and consistency can always be reached.

## Usage

```
migrate -db my_database -dir db/migrations
```

Inside your `db/migrations` directory, you could have the following files:

```
1_create_users.sql
2_modify_users.py
3_update_users.go
```

In `1_create_users.sql` we'd have a normal SQL file.

In `2_modify_users.py` we'd set `#!/usr/bin/python` as the first line.

In `3_update_users.go` we'd have:

```
package main

func main() {
	// Your database migration here
}
```

*Note on numbering:* To enforce that no migration is inserted earlier in
history, Migrate requires that migration files start with ordered numbers. This
can be `1`, `2`, `3` as above, or it can be a timestamp like `YYYYMMDD##`,
such as `2018060101`.

Run `migrate -h` for available flags.
