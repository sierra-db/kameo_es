{
  description = "Dev shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    { nixpkgs, rust-overlay, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (
        system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs {
            inherit system overlays;
            config.allowUnfree = true;
          };
          rust-bin = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rustfmt"
              "rust-analyzer"
            ];
          };
        in
        {
          default = pkgs.mkShell {
            buildInputs =
              with pkgs;
              [
                # openssl
                # pkg-config
                bacon
                rust-bin
                protobuf
              ]
              ++ pkgs.lib.optionals pkgs.stdenv.isDarwin (
                with pkgs.darwin.apple_sdk.frameworks;
                [
                  CoreServices
                  SystemConfiguration
                ]
              );

          };
        }
      );
    };
}
