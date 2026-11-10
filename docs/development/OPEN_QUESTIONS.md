# Questions for the project owner

These questions do not block the autonomous work requested by the user.

1. Confirm the eventual network adapter's additional /v0 collection/scan response
   shapes against the now-available C++ facade and pending file/scanner backend.
   Decision now: expose a documented mock contract, retain the native DTO/recovery
   handoff notes in COORDINATOR.md, and keep clients outside private core headers.
2. Confirm production brand text and whether bows/kisses/sparkles should be the
   default for all users. Decision now: pink theme enabled, all decoration and
   brand copy configurable at runtime; reduced-motion and no-decoration options.
3. Provide a qualified Linux ext4 test device for real power-loss campaigns.
   Decision now: never run destructive tests on this macOS/user workspace; report
   real power qualification NOT RUN and keep generated evidence clearly modeled.
4. Decide later whether browser code must be restricted to Python-generated UI.
   Decision now: follow the revised browser/npm RFC with small dependency-free
   ES modules, no frontend compilation needed for configuration/source edits.
5. Should workbench configuration be editable through a privileged UI endpoint?
   Decision now: edit hot-loaded JSON on disk; browser preview/download is allowed,
   but unauthenticated write-to-server-config is not introduced.

## Assumptions and issues

- The source repository at start contains no engine/HTTP implementation. The
  missing collaborators' interfaces are mocked explicitly, per user instruction.
- Existing fork/PR instructions authorize fork creation, branch commits, push and
  opening a PR into upstream main. No upstream branch or direct main push.
- The machine provides Python 3.13/3.14 and Apple Clang, but initially lacks Ninja
  and LLVM18 quality tools. Dependency/install/test issues are recorded with outcomes.
- Authoring/review status and executed qualification are kept separate; this
  client/mock work alone does not qualify the production durable C++ database.
