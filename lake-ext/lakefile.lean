import Lake
open Lake DSL

package «lake-ext»

@[default_target]
lean_exe «lake-ext» where
  root := `Main
  supportInterpreter := true
