# migrate

`migrate` is a database migration tool for MySQL. It's a new take on database
migrations, designed to satisfy the needs of real projects without the
drawbacks of other designs.

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

`migrate` also takes a different approach to what it considers a migration. Most
of the time to migrate the database you only need SQL, and `migrate` allows you
to use standard SQL files. However when business requirements change, a
database may need to restructure its data without losing any, and SQL may not
be the best or easiest tool for the job. It's critical to include
transformation steps like these in the migration history to achieve consistent
state, so `migrate` allows you to use any program to perform migrations, such as
go, python, node, or ruby. Simply add a `#!` (shebang) instruction to the top
of the file to execute. The one exception is any `.go` file, which should not
include a shebang instruction.

Other tools for database migrations introduced the concept of "up" and "down"
migrations. There are several drawbacks to that approach, but the biggest by
far is the following: "down" migrations may incorrectly reverse an "up"
migration due to programmer error, leaving the database in an inconsistent
state across environments.

Thus, "down" migrations compromise the entire point of any migration tool,
which is above all else to guarantee consistent state. At the same time, it's
possible to write every "down" migration as an "up" migration: simply write the
migration to be performed as another explicit step in the database's history.

Thus, `migrate` eliminates the concept of directional migrations altogether. With
`migrate`, every migration is an "up" migration -- every migration moves you
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
4_create_userpermissions_enum.sql
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

In `4_create_userpermissions_enum.sql`, we'd create and populate an enum table
with serveral `INSERT` statements. It's good practice to populate enum tables
using migrations, since you'll want consistent enums across environments.

*Note on numbering:* To enforce that no migration is inserted earlier in
history, `migrate` requires that migration filenames start with ordered
numbers. This can be `1`, `2`, `3` as above, or it can be a UNIX timestamp or
even a formatted timestamp like `YYYYMMDD##`, such as `2018060101`.

Run `migrate -h` for available flags.

## How to use migrate with an existing database

First, ensure that all your migration file names are numbered as described
above. Then run `migrate` with the `-skip` flag. For example, if a project had
71 migrations that had already been run and another several migrations which
had not, you'd run:

```
migrate -db my_database -dir db/migrations -skip 71_the_last_migration_you_ran.sql
```

Adding `-skip` will populate the `meta` table with the history to that point,
and then run all migrations beyond that point. You only need to pass the
`-skip` flag one time per database.
