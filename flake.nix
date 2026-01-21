{
  description = "Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShell = with pkgs; mkShell {
          name = "rust-env";
          nativeBuildInputs = [
            rustc
            cargo
            cargo-nextest
            pkg-config
          ];
          buildInputs = [
            openssl
          ];

          # make source available to rust-analyzer
          env.RUST_SRC_PATH = pkgs.rustPlatform.rustLibSrc;
        };
      }
    );
}