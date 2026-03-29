{
  description = "sqllineage dev shell — Rust + Python + maturin";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, rust-overlay }:
    let
      systems = [ "aarch64-darwin" "x86_64-darwin" "x86_64-linux" "aarch64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f system);
    in {
      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };

          rust-toolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [ "rust-src" "rust-analyzer" "clippy" "rustfmt" ];
          };

          python = pkgs.python312;
        in {
          default = pkgs.mkShell {
            packages = [
              rust-toolchain
              python
              pkgs.maturin
              pkgs.cargo-nextest
            ];

            env = {
              RUST_BACKTRACE = "1";
              PYO3_PYTHON = "${python}/bin/python3";
            };

            shellHook = ''
              if [ ! -d .venv ]; then
                python3 -m venv .venv
              fi
              source .venv/bin/activate
            '';
          };
        });
    };
}
