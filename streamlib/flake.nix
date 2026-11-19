{
  description = "streamlib dev shell: Rust toolchain + Postgres 17 for the integration tests";

  # Unstable channel, because the code needs rustc 1.88 or newer.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { nixpkgs, ... }:
    let
      systems = [ "aarch64-darwin" "x86_64-darwin" "x86_64-linux" "aarch64-linux" ];
      each = f: nixpkgs.lib.genAttrs systems (s: f nixpkgs.legacyPackages.${s});
    in {
      devShells = each (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            rustc
            cargo
            clippy
            rustfmt
            rust-analyzer
            postgresql_17
          ] ++ lib.optionals stdenv.hostPlatform.isDarwin [ libiconv ];

          # Standard library sources for rust-analyzer.
          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";

          # Keep the database outside the repo, since its socket breaks `path:` flakes.
          shellHook = ''
            export PGHOST="''${XDG_CACHE_HOME:-$HOME/.cache}/streamlib/pg"
            export PGDATA="$PGHOST/data"
            export PGPORT=55432
            export STREAMLIB_PG_DSN="postgresql://streamlib:streamlib@127.0.0.1:$PGPORT/streamlib"
          '';
        };
      });
    };
}
