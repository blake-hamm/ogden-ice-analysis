{
  description = "Minimal flake for ogden_ice_analysis";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

    outputs = { nixpkgs, self, ... } @ inputs: inputs.flake-parts.lib.mkFlake { inherit inputs; } {
        perSystem = { system, pkgs, lib, ... }:
        let
            pkgs = import inputs.nixpkgs {
                inherit system;
                config.allowUnfree = true;
            };
        in {
            devShells.default =
                pkgs.mkShell rec {
                    buildInputs = with pkgs; [
                        uv
                        python313
                        python313Packages.jupyterlab
                        python313Packages.pip
                        ruff
                    ];

                    # Required for building C extensions
                    LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib";
                    # PYTHONPATH is overridden with contents from e.g. poetry */site-package.
                    # We do not want them to be in PYTHONPATH.
                    # Therefore, in ./.envrc PYTHONPATH is set to the _PYTHONPATH defined below
                    # and also in shellHooks (direnv does not load shellHook exports, always).
                    _PYTHONPATH = "${pkgs.python313}/lib/python3.13/site-packages";

                    shellHook = ''
                        # Specify settings.env file
                        set -a
                        source .env
                        set +a

                        # Setup pre-commit
                        pre-commit install
                    '';
                };
        };
        flake = { };
        systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
    };
}