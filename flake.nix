{
  outputs = { self, nixpkgs }:
    let
      nameValuePair = name: value: { inherit name value; };
      genAttrs = names: f: builtins.listToAttrs (map (n: nameValuePair n (f n)) names);
      allSystems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

      forAllSystems = f: genAttrs allSystems (system: f rec {
        inherit system;
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;
      });
    in
    {
      devShells = forAllSystems ({ system, pkgs, ... }: {
        default = with pkgs;
          pkgs.mkShell {
            name = "riff-shell";
            buildInputs = [
              bashInteractive
              cargo rustc rustfmt
              rust-analyzer
            ] ++ lib.optionals (stdenv.isDarwin) [
              libiconv
            ];

            "RUST_SRC_PATH" = "${rustPlatform.rustLibSrc}";


          };
      });
      packages = forAllSystems ({ system, pkgs, ... }: {
        default = pkgs.rustPlatform.buildRustPackage {
            pname = "fync";
            version = "0.1.0";
            src = ./.;
            cargoLock = {
                lockFile = ./Cargo.lock;
            };
        };
      });
      # Compatibility with older Nix installations that don't check for `devShells.<arch>.default` first.
      devShell = forAllSystems ({ system, ... }: self.devShells.${system}.default);
  };
}
