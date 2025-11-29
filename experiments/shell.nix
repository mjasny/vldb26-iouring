let
  pinned = fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/refs/heads/nixos-24.05.tar.gz";
  };
  pkgs = import pinned {
    config = {
      permittedInsecurePackages = [ "v8-9.7.106.18" ];
    };
  };

  mmtable = pkgs.rPackages.buildRPackage {
    name = "mmtable";
    src = pkgs.fetchFromGitHub {
      owner  = "ianmoran11";
      repo   = "mmtable2";
      rev    = "master"; # better: pin to a commit
      sha256 = "sha256-afLM5zk2PktLZBOG4fzCSEVUYxY5YMPb3spe5lNCSBI=";
    };
    propagatedBuildInputs = with pkgs.rPackages; [
      dplyr tidyr purrr gt magrittr broom tibble stringr htmltools xml2 rlang forcats zoo
    ];
  };

  emptyRLib = pkgs.runCommand "empty-r-lib" {} ''
    mkdir -p "$out"
  '';
in
pkgs.mkShell {
  nativeBuildInputs = [ pkgs.bashInteractive pkgs.pkg-config ];
  buildInputs = with pkgs; [
    glibcLocales
    R mmtable pandoc
    v8
    rPackages.V8
    rPackages.gtable
    rPackages.ggplot2
    rPackages.sqldf
    rPackages.patchwork
    rPackages.viridis
    rPackages.slider
    rPackages.data_table
    rPackages.tidytable
    rPackages.ggplotify
    rPackages.ggrepel
    rPackages.ggpattern
    rPackages.ggh4x
    rPackages.geomtextpath
    rPackages.xtable
  ];

  R_LIBS_USER = emptyRLib;     # <- replaces the default user lib
  R_PROFILE_USER = "";
  R_ENVIRON_USER = "";

   # Default C English with UTF-8
  LOCALE_ARCHIVE = "${pkgs.glibcLocales}/lib/locale/locale-archive";
  LANG = "C.UTF-8";
  LC_ALL = "C.UTF-8";

  # (Optional) explicitly set each LC_* if something overrides them
  LC_CTYPE = "C.UTF-8";
  LC_COLLATE = "C.UTF-8";
  LC_MESSAGES = "C.UTF-8";
  LC_MONETARY = "C.UTF-8";
  LC_PAPER = "C.UTF-8";
  LC_MEASUREMENT = "C.UTF-8";
}

