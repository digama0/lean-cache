import Lake.CLI.Main
import Lake.Util.MainM
open Lake Lean

deriving instance ToJson for LakeInstall
deriving instance ToJson for LeanInstall
deriving instance ToJson for Lake.Env
deriving instance ToJson for BuildType
deriving instance ToJson for PackageConfig
deriving instance ToJson for Source
instance [ToJson α] : ToJson (NameMap α) where
  toJson m := .mkObj <| m.toList.map (fun (n, s) => (n.toString, toJson s))
deriving instance ToJson for Dependency
deriving instance ToJson for Glob
instance : ToJson (ModuleFacet α) := ⟨fun m => toJson m.name⟩
deriving instance ToJson for LeanLibConfig
deriving instance ToJson for LeanExeConfig

structure PackageManifest where
  dir : FilePath
  config : PackageConfig
  remoteUrl? : Option String
  gitTag? : Option String
  deps : Array Dependency
  leanLibConfigs : NameMap LeanLibConfig
  leanExeConfigs : NameMap LeanExeConfig
  defaultTargets : Array Name
  deriving ToJson

def Lake.Package.toManifest (pkg : Package) : IO PackageManifest :=
  return { pkg with deps := ← IO.ofExcept <| loadDepsFromEnv pkg.configEnv pkg.leanOpts }

structure WorkspaceManifest where
  root : Name
  packages : Array PackageManifest
  lakeEnv : Lake.Env
  deriving ToJson

def Lake.Workspace.toManifest (ws : Workspace) : IO WorkspaceManifest :=
  return {
    root := ws.root.name
    lakeEnv := ws.lakeEnv
    packages := ← ws.packageArray.mapM (·.toManifest)
  }

def main : IO Unit := do
  let (leanInstall?, lakeInstall?) ← findInstall?
  let config ← MonadError.runEIO <| mkLoadConfig.{0} { leanInstall?, lakeInstall? }
  let ws ← MonadError.runEIO <| (loadWorkspace config).run (.eio .normal)
  let trace ← computeHash (TextFilePath.mk config.configFile)
  IO.FS.createDirAll ws.root.buildDir
  let traceFile := ws.root.buildDir / "workspace-manifest.trace"
  IO.FS.writeFile traceFile trace.toString
  let jsonFile := ws.root.buildDir / "workspace-manifest.json"
  IO.FS.writeFile jsonFile <| toString <| toJson (← ws.toManifest)
  if ws.root.config.buildDir != defaultBuildDir then
    throw <| .userError "non-default build directory not supported"
