{
  description = "A Nix-flake-based Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/release-25.05";
    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      nixpkgs-unstable,
      rust-overlay,
    }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forEachSupportedSystem =
        f:
        nixpkgs.lib.genAttrs supportedSystems (
          system:
          f {
            pkgs = import nixpkgs {
              inherit system;
              overlays = [
                rust-overlay.overlays.default
                self.overlays.default
              ];
            };
            unstable = import nixpkgs-unstable {
              inherit system;
              overlays = [
                rust-overlay.overlays.default
                self.overlays.default
              ];
            };
          }
        );
    in
    {
      overlays.default = final: prev: {
        rustToolchain =
          let
            rust = prev.rust-bin;
            overrides = {
              extensions = [
                "rust-src"
                "rustfmt"
                "rust-analyzer"
              ];
            };
          in
          if builtins.pathExists ./rust-toolchain.toml then
            (rust.fromRustupToolchainFile ./rust-toolchain.toml).override overrides
          else if builtins.pathExists ./rust-toolchain then
            (rust.fromRustupToolchainFile ./rust-toolchain).override overrides
          else
            rust.stable.latest.default.override overrides;
      };

      devShells = forEachSupportedSystem (
        { pkgs, unstable }:
        {
          default = pkgs.mkShell {
            nativeBuildInputs = with pkgs; [
              pkg-config
            ];
            packages =
              with pkgs;
              [
                llvmPackages.bintools
                rustToolchain
                openssl_3.dev
                #rust-analyzer
                cargo-watch
                pkg-config
                cargo-deny
                cargo-edit
                krb5Full
                openssl
                libxml2
                bacon
                zstd
                libz
                lz4
                rc
              ]
              ++ lib.optionals stdenv.hostPlatform.isDarwin [ libiconv ]
              ++ lib.optionals stdenv.hostPlatform.isLinux [
                gdb
                krb5Full.dev
              ];

            # NIX LD Env vars so postgresql_embedded can run with Nix based dependencies
            # (ie. on NixOS)
            NIX_LD_LIBRARY_PATH =
              with pkgs;
              pkgs.lib.makeLibraryPath [
                stdenv.cc.cc
                krb5Full
                openssl
                libxml2
                zstd
                lz4
                libz
              ];
            NIX_LD = pkgs.lib.fileContents "${pkgs.stdenv.cc}/nix-support/dynamic-linker";
            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          };
        }
      );
    };
}
