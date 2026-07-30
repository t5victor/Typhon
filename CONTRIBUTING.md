# Contributing

Keep changes narrow and preserve the domain language. A command expresses a
decision; an event records a fact. Generic CRUD names do not belong in either
catalogue.

Before opening a pull request:

- run `bazel test //...`;
- add replay coverage whenever an aggregate or event contract changes;
- add an integration check when changing PostgreSQL, Kafka, migrations or a
  worker lifecycle;
- keep local credentials, generated outputs and Docker volumes out of Git.

Commands and events live in separate directories by design. Tests belong with
the domain or projection they exercise, never inside a command directory.
