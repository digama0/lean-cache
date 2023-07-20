// #![allow(unused)]
use leangz::lgz::{mix_hash, str_hash, NAME_ANON_HASH as NIL_HASH};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::str::Chars;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// #[derive(Debug, Deserialize)]
// #[serde(rename_all = "camelCase")]
// enum PackageEntry {
//     Path {
//         name: String,
//         dir: PathBuf,
//     },
//     Git {
//         name: String,
//         url: String,
//         rev: String,
//         #[serde(rename = "inputRev?")]
//         input_rev: Option<String>,
//         #[serde(rename = "subDir?")]
//         subdir: Option<PathBuf>,
//     },
// }

#[derive(Clone, Hash, PartialEq, Eq)]
enum NameS {
    Str(Name, String),
}
impl Debug for NameS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Str(Name::Anon, s) => write!(f, "{s}"),
            Self::Str(Name::Next(p), s) => write!(f, "{p:?}.{s}"),
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
enum Name {
    Anon,
    Next(Arc<NameS>),
}
impl Debug for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Anon => write!(f, "[anonymous]"),
            Self::Next(s) => write!(f, "`{s:?}"),
        }
    }
}
impl<'de> Deserialize<'de> for Name {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Name::from_str(&String::deserialize(de)?))
    }
}

impl Name {
    fn str(p: Name, s: String) -> Name {
        Name::Next(Arc::new(NameS::Str(p, s)))
    }

    fn from_str(s: &str) -> Name {
        let mut ch = s.chars();
        let n = parse_name(&mut ch);
        assert!(ch.next().is_none());
        n
    }

    fn len(&self) -> usize {
        let mut p = self;
        let mut size = 0;
        loop {
            match p {
                Name::Anon => break size,
                Name::Next(n) => match &**n {
                    NameS::Str(p2, _) => {
                        size += 1;
                        p = p2;
                    }
                },
            }
        }
    }

    fn parent(&self) -> &Name {
        match self {
            Name::Anon => self,
            Name::Next(p) => match &**p {
                NameS::Str(p, _) => p,
            },
        }
    }

    fn head(&self) -> Option<&String> {
        let mut p = self;
        let mut last = None;
        loop {
            match p {
                Name::Anon => break last,
                Name::Next(n) => match &**n {
                    NameS::Str(p2, s) => {
                        p = p2;
                        last = Some(s)
                    }
                },
            }
        }
    }

    fn is_prefix_of(&self, mut other: &Name, strict: bool) -> bool {
        let l1 = self.len();
        let mut l2 = other.len();
        if l1 > l2 || (strict && l1 == l2) {
            return false;
        }
        while l2 > l1 {
            other = other.parent();
            l2 -= 1;
        }
        self == other
    }

    fn push_path(&self, buf: &mut PathBuf) {
        if let Name::Next(p) = self {
            match &**p {
                NameS::Str(p, s) => {
                    p.push_path(buf);
                    buf.push(s);
                }
            }
        }
    }

    fn to_path(&self, base: &Path, ext: &str) -> PathBuf {
        let mut buf = base.to_owned();
        self.push_path(&mut buf);
        buf.set_extension(ext);
        buf
    }
}
impl From<&str> for Name {
    fn from(value: &str) -> Self {
        Self::str(Self::Anon, value.into())
    }
}

fn is_letter_like(c: char) -> bool {
    let c = c as u32;
    ((0x3b1..=0x3c9).contains(&c) && c != 0x3bb)                       // Lower greek, but lambda
        || ((0x391..=0x3A9).contains(&c) && c != 0x3A0 && c != 0x3A3)  // Upper greek, but Pi and Sigma
        || (0x3ca..=0x3fb).contains(&c)                                // Coptic letters
        || (0x1f00..=0x1ffe).contains(&c)                              // Polytonic Greek Extended Character Set
        || (0x2100..=0x214f).contains(&c)                              // Letter like block
        || (0x1d49c..=0x1d59f).contains(&c) // Latin letters, Script, Double-struck, Fractur
}

fn is_subscript_alphanumeric(c: char) -> bool {
    let c = c as u32;
    (0x2080..=0x2089).contains(&c)
        || (0x2090..=0x209c).contains(&c)
        || (0x1d62..=0x1d6a).contains(&c)
}
fn is_id_first(c: char) -> bool {
    c.is_alphabetic() || c == '_' || is_letter_like(c)
}
fn is_id_rest(c: char) -> bool {
    c.is_ascii_alphanumeric()
        || !matches!(c, '.' | '\n' | ' ')
            && (matches!(c, '_' | '\'' | '!' | '?')
                || is_letter_like(c)
                || is_subscript_alphanumeric(c))
}

fn parse_name(it: &mut Chars<'_>) -> Name {
    let mut p = Name::Anon;
    loop {
        let orig = it.as_str();
        let c = it.next().unwrap_or_else(|| panic!("expected identifier"));
        if c == '«' {
            let s = it.as_str();
            let (i, _) = (s.char_indices())
                .find(|&(_, c)| c == '»')
                .unwrap_or_else(|| panic!("unterminated identifier escape"));
            let (ident, s) = s.split_at(i);
            p = Name::str(p, ident.into());
            *it = s.chars();
            it.next();
        } else {
            assert!(is_id_first(c), "expected identifier");
            let i = (orig.char_indices())
                .find(|&(_, c)| !is_id_rest(c))
                .map_or(orig.len(), |(i, _)| i);
            let (ident, s) = orig.split_at(i);
            p = Name::str(p, ident.into());
            *it = s.chars();
        }
        let is_id_cont = {
            let mut it2 = it.clone();
            it2.next() == Some('.') && matches!(it2.next(), Some(c) if is_id_first(c) || c == '«')
        };
        if !is_id_cont {
            break;
        }
        it.next();
    }
    p
}

fn parse_imports(s: &str) -> Vec<(Name, bool)> {
    fn ws(it: &mut Chars<'_>) {
        let mut orig = it.as_str();
        while let Some(c) = it.next() {
            match c {
                '\t' => panic!("tabs are not allowed"),
                ' ' | '\r' | '\n' => {}
                '-' if it.clone().next() == Some('-') => {
                    it.find(|&c| c == '\n');
                }
                '/' if {
                    let mut chars = it.clone();
                    chars.next() == Some('-') && !matches!(chars.next(), Some('!' | '-'))
                } =>
                {
                    let mut nesting = 1;
                    loop {
                        match it.next().unwrap_or_else(|| panic!("unterminated comment")) {
                            '-' if it.clone().next() == Some('/') => {
                                it.next();
                                if nesting == 1 {
                                    break;
                                }
                                nesting -= 1;
                            }
                            '/' if it.clone().next() == Some('-') => {
                                it.next();
                                nesting += 1;
                            }
                            _ => {}
                        }
                    }
                }
                _ => {
                    *it = orig.chars();
                    break;
                }
            }
            orig = it.as_str();
        }
    }

    fn keyword(it: &mut Chars<'_>, k: &str) -> Option<()> {
        *it = it.as_str().strip_prefix(k)?.chars();
        ws(it);
        Some(())
    }

    let mut it = s.chars();
    let mut imports = vec![];
    ws(&mut it);
    if keyword(&mut it, "prelude").is_none() {
        imports.push(("Init".into(), false))
    }
    while keyword(&mut it, "import").is_some() {
        let rt = keyword(&mut it, "runtime").is_some();
        let p = parse_name(&mut it);
        ws(&mut it);
        imports.push((p, rt))
    }
    imports
}

// fn parse_lake_manifest() -> HashMap<String, PathBuf> {
//     #[derive(Debug, Deserialize)]
//     #[serde(rename_all = "camelCase")]
//     struct Manifest {
//         version: u32,
//         packages_dir: Option<String>,
//         packages: Vec<PackageEntry>,
//     }

//     let mut bytes = Vec::new();
//     File::open("lake-manifest.json")
//         .unwrap()
//         .read_to_end(&mut bytes)
//         .unwrap();
//     let manifest: Manifest = serde_json::from_slice(&bytes).unwrap();
//     assert!(
//         matches!(manifest.version, 3 | 4),
//         "unknown manifest version"
//     );
//     let mut deps = HashMap::new();
//     let rel_pkgs_dir = PathBuf::from("lake-packages");
//     for entry in manifest.packages {
//         match entry {
//             PackageEntry::Path { name, dir } => {
//                 deps.insert(name, dir);
//             }
//             PackageEntry::Git { name, subdir, .. } => {
//                 let mut path = rel_pkgs_dir.join(&name);
//                 if let Some(dir) = subdir {
//                     path.push(dir)
//                 }
//                 deps.insert(name, path);
//             }
//         }
//     }
//     deps
// }

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum BuildType {
    Debug,
    RelWithDebInfo,
    MinSizeRel,
    Release,
}

// #[derive(Debug, Deserialize)]
// #[serde(rename_all = "camelCase")]
// struct WorkspaceConfig {
//     packages_dir: Option<PathBuf>,
// }

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeanConfig {
    // build_type: BuildType,
    more_lean_args: Vec<String>,
    // weak_lean_args: Vec<String>,
    // more_leanc_args: Vec<String>,
    // more_link_args: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PackageConfig {
    // #[serde(flatten)]
    // workspace: WorkspaceConfig,
    #[serde(flatten)]
    lean: LeanConfig,
    name: String,
    // manifest_file: String,
    // extra_dep_targets: Vec<Name>,
    // precompile_modules: bool,
    // more_server_args: Vec<String>,
    src_dir: PathBuf,
    build_dir: PathBuf,
    #[serde(rename = "oleanDir")]
    rel_olean_dir: PathBuf,
    #[serde(skip)]
    olean_dir: PathBuf,
    #[serde(rename = "irDir")]
    rel_ir_dir: PathBuf,
    #[serde(skip)]
    ir_dir: PathBuf,
    // lib_dir: PathBuf,
    // bin_dir: PathBuf,
    // #[serde(rename = "releaseRepo?")]
    // release_repo: Option<String>,
    // #[serde(rename = "buildArchive?")]
    // build_archive: Option<String>,
    // prefer_release_build: bool,
    #[serde(skip)]
    lean_args_trace: u64,
}

// #[derive(Debug, Deserialize)]
// #[serde(rename_all = "camelCase")]
// enum Source {
//     Path {
//         dir: PathBuf,
//     },
//     Git {
//         url: String,
//         rev: String,
//         sub_dir: Option<PathBuf>,
//     },
// }

// #[derive(Debug, Deserialize)]
// #[serde(rename_all = "camelCase")]
// struct Dependency {
//     name: Name,
//     src: Source,
//     options: BTreeMap<String, String>,
// }

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Glob {
    One(Name),
    Submodules(Name),
    AndSubmodules(Name),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeanLibConfig {
    #[serde(flatten)]
    lean: LeanConfig,
    // name: String,
    src_dir: PathBuf,
    roots: Vec<Name>,
    globs: Vec<Glob>,
    // lib_name: String,
    precompile_modules: bool,
    // default_facets: Vec<String>,
    // native_facets: Vec<String>,
    #[serde(skip)]
    lean_args_trace: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeanExeConfig {
    #[serde(flatten)]
    lean: LeanConfig,
    // name: String,
    root: Name,
    // exe_name: String,
    // support_interpreter: bool,
    #[serde(skip)]
    lean_args_trace: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PackageManifest {
    dir: PathBuf,
    config: PackageConfig,
    // #[serde(rename = "remoteUrl?")]
    // remote_url: Option<String>,
    // #[serde(rename = "gitTag?")]
    // git_tag: Option<String>,
    // deps: Vec<Dependency>,
    lean_lib_configs: BTreeMap<String, LeanLibConfig>,
    lean_exe_configs: BTreeMap<String, LeanExeConfig>,
}

// #[derive(Debug, Deserialize)]
// #[serde(rename_all = "camelCase")]
// struct LakeInstall {
//     home: PathBuf,
//     src_dir: PathBuf,
//     lib_dir: PathBuf,
//     lake: PathBuf,
// }

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeanInstall {
    // sysroot: PathBuf,
    githash: String,
    // src_dir: PathBuf,
    // lean_lib_dir: PathBuf,
    // include_dir: PathBuf,
    // system_lib_dir: PathBuf,
    // lean: PathBuf,
    // leanc: PathBuf,
    // shared_lib: PathBuf,
    // ar: PathBuf,
    // cc: PathBuf,
    // custom_cc: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LakeEnv {
    // lake: LakeInstall,
    lean: LeanInstall,
    // lean_path: Vec<PathBuf>,
    // lean_src_path: Vec<PathBuf>,
    // shared_lib_path: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkspaceManifest {
    // root: String,
    packages: Vec<PackageManifest>,
    lake_env: LakeEnv,
}

fn hash_text(s: &str) -> u64 {
    str_hash(s.replace("\r\n", "\n").as_bytes())
}

fn hash_text_file(path: &Path) -> u64 {
    hash_text(&std::fs::read_to_string(path).unwrap())
}

fn hash_bin_file(path: &Path) -> u64 {
    let mut buf = vec![];
    File::open(path).unwrap().read_to_end(&mut buf).unwrap();
    str_hash(&buf)
}

fn hash_bin_file_cached(path: &Path, new_ext: &str) -> u64 {
    let mut trace = path.to_owned();
    trace.set_extension(new_ext);
    if let Ok(trace) = std::fs::read_to_string(&trace) {
        trace.parse::<u64>().unwrap()
    } else {
        let hash = hash_bin_file(path);
        let _ = std::fs::write(trace, format!("{hash}"));
        hash
    }
}

fn read_trace_file(path: &Path, expected: u64) -> bool {
    if let Ok(trace) = std::fs::read_to_string(path) {
        expected == trace.parse::<u64>().unwrap()
    } else {
        false
    }
}

fn parse_workspace_manifest() -> WorkspaceManifest {
    assert!(
        read_trace_file(
            "build/workspace-manifest.trace".as_ref(),
            mix_hash(NIL_HASH, hash_text_file("lakefile.lean".as_ref()))
        ),
        "workspace-manifest hash does not match, run 'lake-ext'"
    );

    let mut bytes = Vec::new();
    File::open("build/workspace-manifest.json")
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    let mut manifest: WorkspaceManifest = serde_json::from_slice(&bytes).unwrap();
    for pkg in &mut manifest.packages {
        pkg.config.src_dir = pkg.dir.join(&pkg.config.src_dir);
        pkg.config.build_dir = pkg.dir.join(&pkg.config.build_dir);
        pkg.config.olean_dir = pkg.config.build_dir.join(&pkg.config.rel_olean_dir);
        pkg.config.ir_dir = pkg.config.build_dir.join(&pkg.config.rel_ir_dir);
        let mut trace = NIL_HASH;
        for arg in &pkg.config.lean.more_lean_args {
            trace = mix_hash(trace, mix_hash(NIL_HASH, str_hash(arg.as_bytes())))
        }
        pkg.config.lean_args_trace = trace;
        for lib in pkg.lean_lib_configs.values_mut() {
            let mut trace = trace;
            for arg in &lib.lean.more_lean_args {
                trace = mix_hash(trace, mix_hash(NIL_HASH, str_hash(arg.as_bytes())))
            }
            lib.lean_args_trace = trace;
            lib.src_dir = pkg.config.src_dir.join(&lib.src_dir);
        }
        for exe in pkg.lean_exe_configs.values_mut() {
            let mut trace = trace;
            for arg in &exe.lean.more_lean_args {
                trace = mix_hash(trace, mix_hash(NIL_HASH, str_hash(arg.as_bytes())))
            }
            exe.lean_args_trace = trace;
        }
    }
    manifest
}

impl Glob {
    fn matches(&self, mod_: &Name) -> bool {
        match self {
            Glob::One(n) => n == mod_,
            Glob::Submodules(n) => n.is_prefix_of(mod_, true),
            Glob::AndSubmodules(n) => n.is_prefix_of(mod_, false),
        }
    }
}

impl LeanLibConfig {
    // fn is_local_module(&self, mod_: &Name) -> bool {
    //     self.roots.iter().any(|root| root.is_prefix_of(mod_, false))
    //         || self.globs.iter().any(|glob| glob.matches(mod_))
    // }

    fn is_buildable_module(&self, mod_: &Name) -> bool {
        self.globs.iter().any(|glob| glob.matches(mod_))
            || self.roots.iter().any(|root| {
                root.is_prefix_of(mod_, false) && self.globs.iter().any(|glob| glob.matches(root))
            })
    }
}

enum LibRef<'a> {
    Lib(&'a LeanLibConfig),
    Exe(&'a LeanExeConfig),
}
impl LibRef<'_> {
    fn precompile_modules(&self) -> bool {
        match *self {
            LibRef::Lib(lib) => lib.precompile_modules,
            LibRef::Exe(_) => false,
        }
    }

    fn lean_args_trace(&self) -> u64 {
        match *self {
            LibRef::Lib(lib) => lib.lean_args_trace,
            LibRef::Exe(exe) => exe.lean_args_trace,
        }
    }
}

impl PackageManifest {
    fn is_local_module(&self, mod_: &Name) -> Option<LibRef<'_>> {
        (self.lean_lib_configs.iter())
            .find_map(|(_, lib)| lib.is_buildable_module(mod_).then_some(LibRef::Lib(lib)))
            .or_else(|| {
                (self.lean_exe_configs.iter())
                    .find_map(|(_, exe)| (*mod_ == exe.root).then_some(LibRef::Exe(exe)))
            })
    }
}
impl WorkspaceManifest {
    fn find_module(&self, mod_: &Name) -> Option<(&PackageManifest, LibRef<'_>)> {
        // fast path
        if let Some(head) = mod_.head() {
            for pkg in &self.packages {
                if let Some(lib) = pkg.lean_lib_configs.get(head) {
                    if lib.is_buildable_module(mod_) {
                        return Some((pkg, LibRef::Lib(lib)));
                    }
                }
            }
        }
        self.packages
            .iter()
            .find_map(|pkg| pkg.is_local_module(mod_).map(|lib| (pkg, lib)))
    }
}

fn hash_list<T>(ls: &[T], mut f: impl FnMut(&T) -> u64) -> u64 {
    let mut hash = 7;
    for t in ls {
        hash = mix_hash(hash, f(t))
    }
    hash
}

fn hash_as_lean(name: &Name) -> u64 {
    fn inner(p: &Name) -> u64 {
        match p {
            Name::Anon => 7,
            Name::Next(p) => {
                let NameS::Str(p, last) = &**p;
                mix_hash(inner(p), str_hash(last.as_bytes()))
            }
        }
    }
    let Name::Next(p) = name else {
        panic!("can't hash [anon]")
    };
    let NameS::Str(p, last) = &**p;
    mix_hash(inner(p), str_hash(format!("{last}.lean").as_bytes()))
}

struct Hasher<'a> {
    ws: &'a WorkspaceManifest,
    root_hash: u64,
    lean_trace: u64,
    invalidate_all: bool,
    cache: HashMap<Name, (Option<u64>, Option<u64>)>,
    to_unpack: Vec<(Name, &'a PackageConfig, u64)>,
}

impl<'a> Hasher<'a> {
    fn new(ws: &'a WorkspaceManifest, invalidate_all: bool) -> Self {
        let mathlib_name = "mathlib";
        let mathlib = (ws.packages.iter())
            .find(|pkg| pkg.config.name == mathlib_name)
            .unwrap_or_else(|| panic!("not in mathlib or a project that depends on mathlib"));
        let root_hash = hash_list(
            &["lakefile.lean", "lean-toolchain", "lake-manifest.json"],
            |file| hash_text_file(&mathlib.dir.join(file)),
        );
        Hasher {
            ws,
            root_hash: mix_hash(7, root_hash),
            lean_trace: mix_hash(NIL_HASH, str_hash(ws.lake_env.lean.githash.as_bytes())),
            invalidate_all,
            cache: HashMap::new(),
            to_unpack: vec![],
        }
    }

    fn get_file_hash(
        &mut self,
        mod_: &Name,
        pkg: &'a PackageManifest,
        lib: LibRef<'_>,
    ) -> (Option<u64>, Option<u64>) {
        if let Some(&result) = self.cache.get(mod_) {
            return result;
        }
        let path = mod_.to_path(&pkg.config.src_dir, "lean");
        let contents = match std::fs::read_to_string(&path) {
            Err(e) if e.kind() == ErrorKind::NotFound => {
                println!(
                    "warning: {} not found. skipping files that depend on it",
                    path.display()
                );
                self.cache.insert(mod_.clone(), (None, None));
                return (None, None);
            }
            e => e.unwrap(),
        };

        let mut hash = self.root_hash;
        hash = mix_hash(hash, hash_as_lean(mod_));
        let src_hash = hash_text(&contents);
        hash = mix_hash(hash, src_hash);
        let mut trace = Some(NIL_HASH);
        if self.invalidate_all || lib.precompile_modules() {
            trace = None; // don't attempt to track transImports etc
        }
        let mut seen = HashSet::new();
        let imports = parse_imports(&contents);
        for (import, _) in &imports {
            if let Some((pkg, lib)) = self.ws.find_module(import) {
                let (Some(import_hash), import_trace) = self.get_file_hash(import, pkg, lib) else {
                    self.cache.insert(mod_.clone(), (None, None));
                    return (None, None);
                };
                hash = mix_hash(hash, import_hash);
                if seen.insert(import) {
                    trace = trace.and_then(|tr| Some(mix_hash(tr, import_trace?)));
                }
            }
        }
        trace = trace.and_then(|import_trace| {
            let mod_trace = NIL_HASH; // since we aren't tracking precompileImports
            let extern_trace = NIL_HASH;
            let dep_trace = mix_hash(import_trace, mix_hash(mod_trace, extern_trace));
            let arg_trace = lib.lean_args_trace();
            let mod_trace = mix_hash(
                self.lean_trace,
                mix_hash(arg_trace, mix_hash(mix_hash(NIL_HASH, src_hash), dep_trace)),
            );
            let trace = read_trace_file(&mod_.to_path(&pkg.config.olean_dir, "trace"), mod_trace);
            trace.then_some(dep_trace)
        });
        if trace.is_none() {
            self.to_unpack.push((mod_.clone(), &pkg.config, hash))
        }
        trace = trace.map(|dep_trace| {
            let olean_hash =
                hash_bin_file_cached(&mod_.to_path(&pkg.config.olean_dir, "olean"), "olean.trace");
            let ilean_hash =
                hash_bin_file_cached(&mod_.to_path(&pkg.config.olean_dir, "ilean"), "ilean.trace");
            mix_hash(mix_hash(olean_hash, ilean_hash), dep_trace)
        });

        self.cache.insert(mod_.clone(), (Some(hash), trace));
        (Some(hash), trace)
    }
}

fn ltar_path(cache_dir: &Path, hash: u64) -> PathBuf {
    cache_dir.join(format!("{hash:016x}.ltar"))
}

const CACHE_URL: &str = "https://lakecache.blob.core.windows.net/mathlib4";
fn cache_url(hash: u64, token: Option<&str>) -> String {
    match token {
        Some(token) => format!("{CACHE_URL}/f/{hash:016x}.ltar?{token}"),
        None => format!("{CACHE_URL}/f/{hash:016x}.ltar"),
    }
}

fn env(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(e) => Some(e),
        Err(std::env::VarError::NotPresent) => None,
        Err(e) => panic!("{e}"),
    }
}

fn cache_dir_from_env() -> PathBuf {
    if let Some(path) = env("XDG_CACHE_HOME") {
        let mut path = PathBuf::from(path);
        path.push("mathlib");
        path
    } else if let Some(path) = env("HOME") {
        let mut path = PathBuf::from(path);
        path.push(".cache");
        path.push("mathlib");
        path
    } else {
        PathBuf::from(".cache")
    }
}

fn get_files_to_unpack(
    ws: &WorkspaceManifest,
    invalidate_all: bool,
    targets: impl IntoIterator<Item = Name>,
) -> Vec<(Name, &PackageConfig, u64)> {
    let mut hasher = Hasher::new(ws, invalidate_all);
    for target in targets {
        if let Some((pkg, lib)) = ws.find_module(&target) {
            hasher.get_file_hash(&target, pkg, lib);
        }
    }
    hasher.to_unpack
}

fn get_cache(force_download: bool, targets: impl IntoIterator<Item = Name>) {
    let ws = parse_workspace_manifest();
    let files_to_unpack = get_files_to_unpack(&ws, force_download, targets);
    if files_to_unpack.is_empty() {
        println!("Nothing to do");
        return;
    }
    let cache_dir = cache_dir_from_env();
    let files_to_download = files_to_unpack.iter().map(|p| p.2);
    let files_to_download: Vec<_> = if force_download {
        files_to_download.collect()
    } else {
        files_to_download
            .filter(|&hash| !ltar_path(&cache_dir, hash).exists())
            .collect()
    };
    if files_to_download.is_empty() {
        println!("Nothing to download");
    } else {
        let size = files_to_download.len();
        std::fs::create_dir_all(&cache_dir).unwrap();
        println!("Attempting to download {size} file(s)");
        let curl_cfg_path = cache_dir.join("curl.cfg");
        {
            let mut curl_cfg = BufWriter::new(File::create(&curl_cfg_path).unwrap());
            for hash in files_to_download {
                writeln!(
                    curl_cfg,
                    "url = {}\n-o {:?}",
                    cache_url(hash, None),
                    ltar_path(&cache_dir, hash).to_str().expect("bad file path")
                )
                .unwrap();
            }
        }
        let mut curl = Command::new("curl")
            .args(["--request", "GET", "--parallel", "--fail", "--silent"])
            .args(["--write-out", "%{json}\n"])
            .args(["--config", curl_cfg_path.to_str().expect("bad path")])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let (mut success, mut failed, mut done, mut last) = (0, 0, 0, Instant::now());
        for line in BufReader::new(curl.stdout.take().unwrap()).lines() {
            #[derive(Debug, Deserialize)]
            struct CurlResult {
                http_code: Option<u32>,
                errormsg: Option<String>,
                filename_effective: Option<String>,
            }

            let result: CurlResult = serde_json::from_str(&line.unwrap()).unwrap();
            match result.http_code {
                Some(200) => success += 1,
                Some(404) => {}
                _ => {
                    failed += 1;
                    if let Some(msg) = result.errormsg {
                        eprintln!("{msg}")
                    }
                    if let Some(file_name) = result.filename_effective {
                        let file = PathBuf::from(file_name);
                        match std::fs::remove_file(file) {
                            Err(e) if e.kind() == ErrorKind::NotFound => {}
                            e => e.unwrap(),
                        }
                    }
                }
            }
            done += 1;
            let now = Instant::now();
            if (now - last) >= Duration::from_millis(100) {
                let percent = 100 * done / size;
                eprint!("\rDownloaded: {success} file(s) [attempted {done}/{size} = {percent}%]");
                if failed != 0 {
                    eprint!(", {failed} failed");
                }
                last = now
            }
        }
        assert!(size == done, "curl failed before reading all files");
        drop(curl);
        std::fs::remove_file(curl_cfg_path).unwrap();
        if done != 0 {
            let percent = 100 * done / size;
            let ok = 100 * success / done;
            eprint!("\rDownloaded: {success} file(s) [attempted {done}/{size} = {percent}%] ({ok}% success)");
            if failed != 0 {
                eprint!(", {failed} failed");
            }
            eprintln!()
        }
        if success + failed < done {
            eprintln!(
                "Warning: some files were not found in the cache.\n\
                This usually means that your local checkout of mathlib4 has diverged from upstream.\n\
                If you push your commits to a branch of the mathlib4 repository, \
                  CI will build the oleans and they will be available later."
            );
        }
        if failed != 0 {
            eprintln!("{failed} download(s) failed");
        }
    }
    let now = Instant::now();
    let to_unpack = files_to_unpack.len();
    eprintln!("Decompressing {to_unpack} file(s)");

    let mut error = AtomicBool::new(false);
    let mut unpacked = AtomicUsize::new(0);
    let fail = || error.store(true, Ordering::Relaxed);
    files_to_unpack
        .into_par_iter()
        .for_each(|(mod_, pkg, hash)| {
            let tarfile = match File::open(ltar_path(&cache_dir, hash)) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
                e => BufReader::new(e.unwrap()),
            };
            if let Err(e) = leangz::ltar::unpack(&pkg.olean_dir, tarfile, false, false) {
                eprintln!("{mod_:?}: {e}");
                fail()
            } else {
                unpacked.fetch_add(1, Ordering::Relaxed);
            }
        });
    eprintln!("unpacked in {} ms", now.elapsed().as_millis());
    if *error.get_mut() {
        eprintln!("one or more unpacking operation failed, see above messages")
    }
    let not_unpacked = to_unpack - *unpacked.get_mut();
    if not_unpacked != 0 {
        eprintln!(
            "{not_unpacked} file(s) did not have cached versions, \
            run `lake build` to rebuild these files"
        )
    }
    std::process::exit(*error.get_mut() as i32);
}

fn get_targets(args: impl Iterator<Item = String>) -> impl Iterator<Item = Name> {
    let mut targets = args.collect::<Vec<_>>();
    if targets.is_empty() {
        targets = vec!["Mathlib".into(), "MathlibExtras".into()];
    }
    targets.into_iter().map(|target| Name::from_str(&target))
}

fn help() {
    println!(
        "\
Mathlib4 caching CLI
Usage: cache [COMMAND]

Commands:
  # No privilege required
  get  [ARGS]  Download linked files missing on the local cache and decompress
  get! [ARGS]  Download all linked files and decompress
  clean        Delete non-linked files
  clean!       Delete everything on the local cache

  # Privilege required
  put          Run 'mk' then upload linked files missing on the server
  put!         Run 'mk' then upload all linked files

* Linked files refer to local cache files with corresponding Lean sources
* Commands ending with '!' should be used manually, when hot-fixes are needed

# The arguments for 'get' and 'get!'

'get' and 'get!' can process list of paths, allowing the user to be more
specific about what should be downloaded. For example, with automatic glob
expansion in shell, one can call:

$ lake exe cache get Mathlib/Algebra/Field/*.lean Mathlib/Data/*.lean

Which will download the cache for:
* Every Lean file inside 'Mathlib/Algebra/Field/'
* Every Lean file inside 'Mathlib/Data/'
* Everything that's needed for the above"
    );
}

fn get_token() -> String {
    env("MATHLIB_CACHE_SAS")
        .expect("environment variable MATHLIB_CACHE_SAS must be set to upload caches")
}

fn put_cache(overwrite: bool, targets: impl IntoIterator<Item = Name>) {
    let token = get_token();
    let hash = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap()
        .stdout;
    let hash = String::from_utf8(hash).unwrap();
    let cache_dir = cache_dir_from_env();
    std::fs::create_dir_all(&cache_dir).unwrap();
    let commit_file = cache_dir.join(hash.trim_end());
    let ws = parse_workspace_manifest();
    let files = get_files_to_unpack(&ws, true, targets);
    let send_commit = || {
        let mut file = BufWriter::new(File::create(&commit_file).unwrap());
        for (_, _, hash) in &files {
            writeln!(file, "{hash:016x}").unwrap()
        }
        drop(file);
        let mut curl = Command::new("curl");
        curl.args(["-X", "PUT", "-H", "x-ms-blob-type: BlockBlob"]);
        if !overwrite {
            curl.args(["-H", "If-None-Match: *"]);
        }
        curl.args(["-T", commit_file.to_str().unwrap()])
            .arg(format!("{CACHE_URL}/c/{hash}?{token}"))
            .status()
            .unwrap();
        std::fs::remove_file(commit_file).unwrap();
    };
    let send_files = || {
        if files.is_empty() {
            println!("No files to upload");
            return;
        }
        (&files).into_par_iter().for_each(|&(ref mod_, pkg, hash)| {
            let tarfile = ltar_path(&cache_dir, hash);
            if !overwrite && tarfile.exists() {
                return;
            }
            let tarfile = BufWriter::new(File::open(tarfile).unwrap());
            let mut paths = vec![
                mod_.to_path(&pkg.rel_olean_dir, "olean"),
                mod_.to_path(&pkg.rel_olean_dir, "ilean"),
                mod_.to_path(&pkg.rel_ir_dir, "c"),
            ];
            let extra = mod_.to_path(&pkg.rel_olean_dir, "extra");
            if pkg.build_dir.join(&extra).exists() {
                paths.push(extra)
            }
            leangz::ltar::pack(
                &pkg.build_dir,
                tarfile,
                mod_.to_path(&pkg.rel_olean_dir, "trace").to_str().unwrap(),
                paths
                    .into_iter()
                    .map(|path| path.to_str().unwrap().to_owned()),
                false,
            )
            .unwrap();
        });

        println!("Attempting to upload {} file(s)", files.len());
        let curl_cfg_path = cache_dir.join("curl.cfg");
        let mut curl_cfg = BufWriter::new(File::create(&curl_cfg_path).unwrap());
        for &(_, _, hash) in &files {
            writeln!(
                curl_cfg,
                "-T {:?}\nurl = {}",
                ltar_path(&cache_dir, hash).to_str().expect("bad file path"),
                cache_url(hash, Some(&token)),
            )
            .unwrap();
        }
        drop(curl_cfg);
        let mut curl = Command::new("curl");
        curl.args(["-X", "PUT", "-H", "x-ms-blob-type: BlockBlob"]);
        if !overwrite {
            curl.args(["-H", "If-None-Match: *"]);
        }
        curl.args(["--parallel", "-K"])
            .arg(curl_cfg_path.to_str().expect("bad path"))
            .status()
            .unwrap();
        std::fs::remove_file(curl_cfg_path).unwrap();
    };
    rayon::join(send_commit, send_files);
}

fn clean_cache(everything: bool) {
    let cache_dir = cache_dir_from_env();
    let rd = match std::fs::read_dir(cache_dir) {
        Err(e) if e.kind() == ErrorKind::NotFound => return,
        e => e.unwrap(),
    };
    let keep = if everything {
        HashSet::new()
    } else {
        let ws = parse_workspace_manifest();
        let files = get_files_to_unpack(&ws, true, get_targets(std::iter::empty()));
        files
            .into_iter()
            .map(|(_, _, hash)| format!("{hash:016x}"))
            .collect()
    };
    let mut to_remove = vec![];
    for entry in rd {
        let Ok(entry) = entry else { continue };
        if let Some(name) = entry.file_name().to_str() {
            if let Some(name) = name.strip_suffix(".ltar") {
                if !keep.contains(name) {
                    to_remove.push(entry.path());
                }
            }
        }
    }
    to_remove
        .into_par_iter()
        .for_each(|path| std::fs::remove_file(path).unwrap())
}

fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("get") => get_cache(false, get_targets(args)),
        Some("get!") => get_cache(true, get_targets(args)),
        Some("put") => put_cache(false, get_targets(args)),
        Some("put!") => put_cache(true, get_targets(args)),
        Some("clean") => clean_cache(false),
        Some("clean!") => clean_cache(true),
        _ => help(),
    }
}
