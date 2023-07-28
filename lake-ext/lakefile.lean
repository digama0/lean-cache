import Lake
open Lake DSL

package «lake-ext»

elab "current_lake" : term <= ty => do
  let some path ← findLeanSysroot? | throwError "no lean?"
    let path := path / "src" / "lean" / "lake"
  Lean.Elab.Term.elabTerm (Lean.quote path.toString : Lean.Term) ty

require Lake from current_lake

@[default_target]
lean_exe «lake-ext» where
  buildType := .debug
  root := `Main
  supportInterpreter := true
