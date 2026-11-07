# Security policy

Please report suspected vulnerabilities privately through GitHub's private vulnerability
reporting on this repository's Security tab. Include a reproduction and tested commit.

The host authenticates callers and authorizes access to database IDs. SqliteHarbor has
no network API or tenant authentication. Cluster members and their Erlang cookies,
cache directories and object-store credentials are trusted. Protect them accordingly.

SQL is restricted using SQLite's authorizer, trusted_schema=OFF, disabled extensions,
one statement per string and value/request/result/database/deadline limits. Report any way
SQL can access another database, local files, the network or execute native code.
The SQLite heap budget is OS-process-wide; this library does not promise hostile-tenant
resource isolation. An authorized caller can destroy its own data, including corrupting
its own FTS shadow tables. Durability loss windows and conflict rescue are documented
product choices, not claims of consensus or fenced ownership.
Binary import sources are trusted local paths; upload bytes belong in a chunk enumerable.
Sort/temp spills use the OS temporary directory. Conflict rescues survive database
deletion until the operator removes them.
