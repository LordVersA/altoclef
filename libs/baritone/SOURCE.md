# Vendored Baritone

`baritone-unoptimized-fabric-1.21.1.jar` was built from source, not downloaded:

- `MiranCZ/baritone_altoclef` branch `1.21.1` at `a34f78abd7e63f0089ade89909b2d48049afb208` (2025-04-28)
- its submodule `cabaletta/baritone` at `aff430ac518efc40ca995f735ece3e3e7907115b` (2025-04-22)
- the four patches in `patches/` applied with `git apply`, then `./gradlew build` with JDK 21
- SHA-256 `129ed53bf78839322bcd49d362b6e36781e0553c816d0a1261cd6c0595b869fd`

Compared with the jar MiranCZ publishes (SHA-256
`2fa4a78e5224920840d98ef20156f9fb05812394f1ba95b78e45d628da2a0aae`) every Baritone class and resource
is byte-identical; only the `fabric.mod.json` that Loom generates for the nested `nether-pathfinder`
jar differs.

Rebuild with `scripts/platform/build-body.sh` in the IO repository.
