# Validates a `cargo metadata` dependency graph against the canonical DogeOS
# component, clean DogeOS Reth, DogeOS REVM, and reviewed official Reth pins.
#
# Arguments (exact canonical source strings):
#   $components  DogeOS component packages (dogeos-reth repository)
#   $reth        clean DogeOS Reth packages (DogeOS69/reth repository)
#   $revm        DogeOS REVM packages (dogeos-revm repository)
#   $official    reviewed official Reth transitive source (paradigmxyz/reth)
#
# Emits one line per violation; empty output means the graph is verified.

# Reduce a source URL to its repository identity so revision pinning cannot be
# bypassed through case, scheme, `.git`, or trailing-slash spelling variants.
def norm_repo:
  ascii_downcase
  | sub("^git\\+"; "")
  | split("?")[0]
  | split("#")[0]
  | sub("^(https?|ssh|git)://"; "")
  | sub("^git@"; "")
  | sub("^github\\.com:"; "github.com/")
  | sub("\\.git$"; "")
  | sub("/+$"; "");

def retired_repos:
  ["github.com/dogeos69/dogeos-reth2",
   "github.com/dogeos69/scroll-reth",
   "github.com/scroll-tech/reth"];

[
  # Anchor packages must each resolve exactly once from their canonical source.
  (if [.packages[] | select(.name == "dogeos-reth-node") | .source] == [$components] then empty
   else "anchor dogeos-reth-node must resolve exactly once from the canonical DogeOS component source" end),
  (if [.packages[] | select(.name == "reth-node-builder") | .source] == [$reth] then empty
   else "anchor reth-node-builder must resolve exactly once from the clean DogeOS Reth source" end),
  (if [.packages[] | select(.name == "revm-scroll") | .source] == [$revm] then empty
   else "revm-scroll must resolve exactly once from the canonical DogeOS REVM source" end),

  (.packages[]
   | (if .source == null then null else (.source | norm_repo) end) as $repo
   | (
       # Every DogeOS component package must use the canonical component
       # source; null/path sources fail this comparison as well.
       (if (.name | startswith("dogeos-")) and .source != $components then
          "DogeOS component package \(.name) must use the canonical component source, found \(.source // "null/path source")"
        else empty end),

       # Every reth-* package must be a registry compatibility crate, clean
       # DogeOS Reth, or the exact reviewed official Reth source.
       (if (.name | test("^reth(-|$)")) then
          (if .source == null then
             "Reth package \(.name) must not use a null/path source"
           elif (.source | startswith("registry+")) or .source == $reth or .source == $official then
             empty
           else
             "Reth package \(.name) has an unreviewed source \(.source)"
           end)
        else empty end),

       # Any package drawn from a guarded repository must use the exact
       # canonical source; retired forks are rejected outright.
       (if $repo == null then empty
        elif retired_repos | index($repo) then
          "package \(.name) uses the retired fork source \(.source)"
        elif $repo == "github.com/dogeos69/dogeos-reth" and .source != $components then
          "package \(.name) uses a noncanonical DogeOS component source \(.source)"
        elif $repo == "github.com/dogeos69/reth" and .source != $reth then
          "package \(.name) uses a noncanonical clean DogeOS Reth source \(.source)"
        elif $repo == "github.com/dogeos69/dogeos-revm" and .source != $revm then
          "package \(.name) uses a noncanonical DogeOS REVM source \(.source)"
        elif $repo == "github.com/paradigmxyz/reth" and .source != $official then
          "package \(.name) uses an unreviewed official Reth source \(.source)"
        else empty end)
     )
  )
]
| .[]
