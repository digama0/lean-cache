use crossbeam_channel::TryRecvError;
use curl::easy::Easy2;
use curl::multi::{Easy2Handle, Multi};
// #![allow(unused)]
use leangz::lgz::{mix_hash, str_hash, NAME_ANON_HASH as NIL_HASH};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
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
//         input_rev: Option<String>,
//         sub_dir: Option<PathBuf>,
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
//             PackageEntry::Git { name, sub_dir, .. } => {
//                 let mut path = rel_pkgs_dir.join(&name);
//                 if let Some(dir) = sub_dir {
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
    name: Name,
    // manifest_file: String,
    extra_dep_targets: Vec<Name>,
    precompile_modules: bool,
    // more_server_args: Vec<String>,
    src_dir: PathBuf,
    build_dir: PathBuf,
    #[serde(rename = "leanLibDir")]
    rel_lean_lib_dir: PathBuf,
    #[serde(skip)]
    lean_lib_dir: PathBuf,
    #[serde(rename = "nativeLibDir")]
    rel_native_lib_dir: PathBuf,
    #[serde(skip)]
    native_lib_dir: PathBuf,
    // bin_dir: PathBuf,
    // ir_dir: PathBuf,
    // release_repo: Option<String>,
    build_archive: Option<String>,
    prefer_release_build: bool,
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Dependency {
    name: Name,
    // src: Source,
    // options: BTreeMap<String, String>,
}

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
    extra_dep_targets: Vec<Name>,
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
    src_dir: PathBuf,
    root: Name,
    // exe_name: String,
    extra_dep_targets: Vec<Name>,
    // support_interpreter: bool,
    #[serde(skip)]
    lean_args_trace: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PackageManifest {
    dir: PathBuf,
    config: PackageConfig,
    // remote_url: Option<String>,
    // git_tag: Option<String>,
    deps: Vec<Dependency>,
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
    root: Name,
    packages: Vec<PackageManifest>,
    lake_env: LakeEnv,
}

impl WorkspaceManifest {
    fn package(&self, name: &Name) -> Option<&PackageManifest> {
        self.packages.iter().find(|p| p.config.name == *name)
    }
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

fn hash_bin_file_cached(path: &Path, new_ext: &str, invalidate: bool) -> u64 {
    let mut trace = path.to_owned();
    trace.set_extension(new_ext);
    if !invalidate {
        if let Ok(trace) = std::fs::read_to_string(&trace) {
            return trace.parse::<u64>().unwrap();
        }
    }
    let hash = hash_bin_file(path);
    let _ = std::fs::write(trace, format!("{hash}"));
    hash
}

fn check_trace_file(path: &Path, expected: u64) -> bool {
    if let Ok(trace) = std::fs::read_to_string(path) {
        expected == trace.parse::<u64>().unwrap()
    } else {
        false
    }
}

fn parse_workspace_manifest() -> WorkspaceManifest {
    assert!(
        check_trace_file(
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
        pkg.config.lean_lib_dir = pkg.config.build_dir.join(&pkg.config.rel_lean_lib_dir);
        pkg.config.native_lib_dir = pkg.config.build_dir.join(&pkg.config.rel_native_lib_dir);
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
            exe.src_dir = pkg.config.src_dir.join(&exe.src_dir);
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

#[derive(Copy, Clone)]
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

    fn extra_dep_targets(&self) -> &[Name] {
        match *self {
            LibRef::Lib(lib) => &lib.extra_dep_targets,
            LibRef::Exe(exe) => &exe.extra_dep_targets,
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

#[derive(Copy, Clone, Debug)]
enum Trace {
    Unknown,
    Ok(u64),
    Changed(u64),
}

impl From<u64> for Trace {
    fn from(v: u64) -> Self {
        Self::Ok(v)
    }
}
impl From<Option<u64>> for Trace {
    fn from(v: Option<u64>) -> Self {
        match v {
            Some(trace) => Self::Ok(trace),
            None => Self::Unknown,
        }
    }
}
impl Trace {
    fn mix(self, other: impl Into<Self>) -> Self {
        match (self, other.into()) {
            (Trace::Unknown, _) | (_, Trace::Unknown) => Trace::Unknown,
            (Trace::Ok(a), Trace::Ok(b)) => Trace::Ok(mix_hash(a, b)),
            (Trace::Changed(a), Trace::Ok(b))
            | (Trace::Ok(a), Trace::Changed(b))
            | (Trace::Changed(a), Trace::Changed(b)) => Trace::Changed(mix_hash(a, b)),
        }
    }

    fn change(self, ch: impl FnOnce(u64) -> bool) -> Self {
        match self {
            Trace::Ok(a) if ch(a) => Trace::Changed(a),
            _ => self,
        }
    }

    fn and_then(self, then: impl FnOnce(u64, bool) -> Trace) -> Trace {
        match self {
            Trace::Unknown => self,
            Trace::Ok(a) => then(a, false),
            Trace::Changed(a) => then(a, true),
        }
    }
}

struct Hasher<'a, F> {
    ws: &'a WorkspaceManifest,
    root_hash: u64,
    lean_trace: u64,
    invalidate_all: bool,
    cache: HashMap<Name, (Option<u64>, Trace)>,
    extra_dep_targets: HashMap<Name, Option<u64>>,
    unpack: F,
}

impl<'a, F: FnMut(Name, &'a PackageConfig, u64, Option<u64>)> Hasher<'a, F> {
    fn new(ws: &'a WorkspaceManifest, invalidate_all: bool, unpack: F) -> Self {
        let mathlib_name = Name::from_str("mathlib");
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
            unpack,
            extra_dep_targets: HashMap::new(),
        }
    }

    fn pkg_extra_dep_targets(&mut self, pkg: &'a PackageManifest) -> Option<u64> {
        if let Some(&result) = self.extra_dep_targets.get(&pkg.config.name) {
            return result;
        }
        let trace = (|| -> Option<u64> {
            if !pkg.config.extra_dep_targets.is_empty() {
                return None;
            }
            let mut trace = NIL_HASH;
            for dep in &pkg.deps {
                let pkg = self.ws.package(&dep.name).unwrap();
                trace = mix_hash(trace, self.pkg_extra_dep_targets(pkg)?)
            }
            if pkg.config.name != self.ws.root && pkg.config.prefer_release_build {
                const OS: &str = if cfg!(windows) {
                    "windows"
                } else if cfg!(darwin) {
                    "macOS"
                } else {
                    "linux"
                };
                const PTR_WIDTH: usize = 8 * std::mem::size_of::<usize>();
                let archive = match &pkg.config.build_archive {
                    Some(name) => format!("{name}-{OS}-{PTR_WIDTH}.tar.gz"),
                    None => format!("{OS}-{PTR_WIDTH}.tar.gz"),
                };
                let path = pkg.config.build_dir.join(archive);
                if !path.exists() {
                    return None;
                }
                trace = mix_hash(
                    trace,
                    hash_bin_file_cached(&path, "gz.hash", self.invalidate_all),
                );
            }
            Some(trace)
        })();
        self.extra_dep_targets
            .insert(pkg.config.name.clone(), trace);
        trace
    }

    fn lib_extra_dep_targets(&mut self, pkg: &'a PackageManifest, lib: LibRef<'_>) -> Option<u64> {
        if !lib.extra_dep_targets().is_empty() {
            return None;
        }
        self.pkg_extra_dep_targets(pkg)
    }

    fn get_file_hash(
        &mut self,
        mod_: &Name,
        pkg: &'a PackageManifest,
        lib: LibRef<'_>,
    ) -> (Option<u64>, Trace) {
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
                self.cache.insert(mod_.clone(), (None, Trace::Unknown));
                return (None, Trace::Unknown);
            }
            e => e.unwrap(),
        };

        let mut hash = self.root_hash;
        hash = mix_hash(hash, hash_as_lean(mod_));
        let src_hash = hash_text(&contents);
        hash = mix_hash(hash, src_hash);
        let mut import_trace = Trace::Ok(NIL_HASH);
        if pkg.config.precompile_modules || lib.precompile_modules() {
            import_trace = Trace::Unknown; // don't attempt to track transImports etc
        }
        let mut seen = HashSet::new();
        let imports = parse_imports(&contents);
        for (import, _) in &imports {
            if let Some((pkg, lib)) = self.ws.find_module(import) {
                let (Some(import_hash), import1_trace) = self.get_file_hash(import, pkg, lib) else {
                    self.cache.insert(mod_.clone(), (None, Trace::Unknown));
                    return (None, Trace::Unknown);
                };
                hash = mix_hash(hash, import_hash);
                if seen.insert(import) {
                    import_trace = import_trace.mix(import1_trace);
                }
            }
        }
        let dep_trace = import_trace.and_then(|_, _| {
            let extra_dep_trace = Trace::from(self.lib_extra_dep_targets(pkg, lib));
            let mod_trace = NIL_HASH; // since we aren't tracking precompileImports
            let extern_trace = NIL_HASH;
            extra_dep_trace.mix(import_trace.mix(mix_hash(mod_trace, extern_trace)))
        });
        let mod_trace = dep_trace.and_then(|dep_trace, ch| {
            let arg_trace = lib.lean_args_trace();
            let mod_trace = mix_hash(
                self.lean_trace,
                mix_hash(arg_trace, mix_hash(mix_hash(NIL_HASH, src_hash), dep_trace)),
            );
            Trace::Ok(mod_trace).change(|_| ch).change(|mod_trace| {
                check_trace_file(&mod_.to_path(&pkg.config.lean_lib_dir, "trace"), mod_trace)
            })
        });
        match mod_trace {
            Trace::Ok(_) if !self.invalidate_all => {}
            Trace::Unknown => (self.unpack)(mod_.clone(), &pkg.config, hash, None),
            Trace::Ok(a) | Trace::Changed(a) => {
                (self.unpack)(mod_.clone(), &pkg.config, hash, Some(a))
            }
        }
        let trace = dep_trace.and_then(|_, _| {
            let olean_hash = hash_bin_file_cached(
                &mod_.to_path(&pkg.config.lean_lib_dir, "olean"),
                "olean.trace",
                self.invalidate_all,
            );
            let ilean_hash = hash_bin_file_cached(
                &mod_.to_path(&pkg.config.lean_lib_dir, "ilean"),
                "ilean.trace",
                self.invalidate_all,
            );
            Trace::Ok(mix_hash(olean_hash, ilean_hash)).mix(dep_trace)
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

fn get_files_to_unpack<'a>(
    ws: &'a WorkspaceManifest,
    invalidate_all: bool,
    targets: impl IntoIterator<Item = Name>,
    f: impl FnMut(Name, &'a PackageConfig, u64, Option<u64>),
) {
    let mut hasher = Hasher::new(ws, invalidate_all, f);
    for target in targets {
        if let Some((pkg, lib)) = ws.find_module(&target) {
            hasher.get_file_hash(&target, pkg, lib);
        }
    }
}
#[derive(Hash, PartialEq, Eq)]
enum CurlError {
    Curl(u32, Option<String>),
    Status(u32),
}

impl CurlError {
    fn report(&self, mod_: &Name) {
        match self {
            CurlError::Curl(code, extra) => {
                let mut e = curl::Error::new(*code);
                if let Some(extra) = extra {
                    e.set_extra(extra.clone());
                }
                eprintln!("{mod_:?} failed:\n{e}");
            }
            CurlError::Status(code) => {
                eprintln!("{mod_:?} failed: HTTP status {code}")
            }
        }
    }
}
struct MultiQueue<H, S> {
    multi: Option<Multi>,
    handles: Vec<Option<Easy2Handle<H>>>,
    active: u32,
    status: S,
}

trait ProcessMsg<H> {
    fn process(&mut self, _easy: Easy2<H>, _result: Result<(), curl::Error>) {}
}

impl<H, S: Default> Default for MultiQueue<H, S> {
    fn default() -> Self {
        Self {
            multi: None,
            handles: vec![],
            active: 0,
            status: S::default(),
        }
    }
}

impl<H, S: ProcessMsg<H>> MultiQueue<H, S> {
    fn push(&mut self, easy: Easy2<H>) {
        let multi = self.multi.get_or_insert_with(|| {
            let mut multi = Multi::new();
            multi.set_max_total_connections(50).unwrap();
            multi
        });
        let mut easy = multi.add2(easy).unwrap();
        easy.set_token(self.handles.len()).unwrap();
        self.handles.push(Some(easy));
        self.active += 1;
        self.perform();
    }

    fn perform(&mut self) -> bool {
        if self.active > 0 {
            let multi = self.multi.as_ref().unwrap();
            self.active = multi.perform().unwrap();
            multi.messages(|msg| {
                if let Some(result) = msg.result() {
                    if let Some(handle) = self.handles[msg.token().unwrap()].take() {
                        let easy = multi.remove2(handle).unwrap();
                        self.status.process(easy, result);
                    }
                }
            });
        }
        self.active != 0
    }

    fn wait(&self, d: Duration) {
        if self.active > 0 {
            if let Some(multi) = &self.multi {
                multi.wait(&mut [], d).unwrap();
            }
        }
    }
}

fn get_cache(force_download: bool, targets: impl IntoIterator<Item = Name>) {
    let ws = parse_workspace_manifest();
    let mut files_to_unpack = vec![];
    let mut multi = MultiQueue::default();
    let cache_dir = cache_dir_from_env();
    struct H {
        mod_: Name,
        temp: PathBuf,
        path: PathBuf,
        file: Option<BufWriter<File>>,
    }
    impl curl::easy::Handler for H {
        fn write(&mut self, data: &[u8]) -> Result<usize, curl::easy::WriteError> {
            self.file
                .get_or_insert_with(|| BufWriter::new(File::create(&self.temp).unwrap()))
                .write_all(data)
                .unwrap();
            Ok(data.len())
        }
    }
    get_files_to_unpack(&ws, force_download, targets, |mod_, pkg, hash, trace| {
        let path = ltar_path(&cache_dir, hash);
        if force_download || !path.exists() {
            if multi.handles.is_empty() {
                std::fs::create_dir_all(&cache_dir).unwrap();
            }
            let mut temp = path.clone();
            temp.set_extension("ltar.part");
            let mut easy = curl::easy::Easy2::new(H {
                mod_: mod_.clone(),
                temp,
                path,
                file: None,
            });
            easy.url(&cache_url(hash, None)).unwrap();
            multi.push(easy);
        }
        files_to_unpack.push((mod_, pkg, hash, trace));
    });
    if files_to_unpack.is_empty() {
        println!("Nothing to do");
        return;
    }
    if multi.handles.is_empty() {
        println!("Nothing to download");
    } else {
        #[derive(Default)]
        struct S {
            failed: usize,
            success: usize,
            done: usize,
            failures: HashMap<CurlError, u32>,
            suppressed: usize,
        }
        impl S {
            fn report(&mut self, mod_: &Name, err: CurlError) {
                self.failed += 1;
                match self.failures.entry(err) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        if *e.get() < 3 {
                            e.key().report(mod_);
                        } else {
                            self.suppressed += 1;
                        }
                        *e.get_mut() += 1;
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.key().report(mod_);
                        e.insert(1);
                    }
                }
            }
        }
        impl ProcessMsg<H> for S {
            fn process(&mut self, mut easy: Easy2<H>, result: Result<(), curl::Error>) {
                let file = easy.get_mut().file.take();
                let ok = match result {
                    Ok(()) => match easy.response_code().unwrap() {
                        200 => true,
                        404 => false,
                        code => {
                            self.report(&easy.get_mut().mod_, CurlError::Status(code));
                            false
                        }
                    },
                    Err(e) => {
                        let msg = easy.take_error_buf();
                        self.report(&easy.get_mut().mod_, CurlError::Curl(e.code(), msg));
                        false
                    }
                };
                if let Some(file) = file {
                    drop(file);
                    let h = easy.get_mut();
                    let _ = if ok {
                        std::fs::rename(&h.temp, &h.path)
                    } else {
                        std::fs::remove_file(&h.temp)
                    };
                }
                if ok {
                    self.success += 1;
                }
                self.done += 1;
            }
        }

        let size = multi.handles.len();
        println!("Attempting to download {size} file(s)");
        let mut last = Instant::now();
        while multi.perform() {
            let S {
                failed,
                success,
                done,
                ..
            } = multi.status;
            let now = Instant::now();
            if (now - last) >= Duration::from_millis(100) {
                let percent = 100 * done / size;
                eprint!("\rDownloaded: {success} file(s) [attempted {done}/{size} = {percent}%]");
                if failed != 0 {
                    eprint!(", {failed} failed");
                }
                last = now
            }
            multi.wait(Duration::from_secs(1));
        }
        let S {
            failed,
            success,
            done,
            suppressed,
            ..
        } = multi.status;
        if suppressed > 0 {
            eprintln!("{suppressed} similar errors were suppressed");
        }
        assert!(size == done, "curl failed before reading all files");
        drop(multi);
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
    let mut unknown_unpack = AtomicUsize::new(0);
    let mut bad_unpack = AtomicUsize::new(0);
    let fail = || error.store(true, Ordering::Relaxed);
    files_to_unpack
        .into_par_iter()
        .for_each(|(mod_, pkg, hash, trace)| {
            let path = ltar_path(&cache_dir, hash);
            let tarfile = match File::open(&path) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
                e => BufReader::new(e.unwrap()),
            };
            match leangz::ltar::unpack(&pkg.lean_lib_dir, tarfile, false, false) {
                Err(e) => {
                    let is_truncated = match e {
                        leangz::ltar::UnpackError::IOError(ref e) => {
                            e.kind() == ErrorKind::UnexpectedEof
                        }
                        leangz::ltar::UnpackError::InvalidUtf8(_) => false,
                        leangz::ltar::UnpackError::BadLtar => true,
                        leangz::ltar::UnpackError::BadTrace => false,
                        leangz::ltar::UnpackError::UnsupportedCompression(_) => false,
                    };
                    if is_truncated {
                        eprintln!("removing corrupted cache file for {mod_:?}");
                        let _ = std::fs::remove_file(path);
                    } else {
                        eprintln!("{mod_:?}: {e}");
                    }
                    fail()
                }
                Ok(trace2) => {
                    if let Some(t) = trace {
                        if t != trace2 {
                            bad_unpack.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        unknown_unpack.fetch_add(1, Ordering::Relaxed);
                    }
                    unpacked.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    eprintln!("unpacked in {} ms", now.elapsed().as_millis());
    if *error.get_mut() {
        eprintln!("one or more unpacking operation failed, see above messages")
    }
    let not_unpacked = to_unpack - *unpacked.get_mut();
    if not_unpacked != 0 {
        eprintln!(
            "note: {not_unpacked} file(s) did not have cached versions;\n\
            run `lake build` to rebuild these files"
        )
    }
    let bad_unpack = *bad_unpack.get_mut();
    if bad_unpack != 0 {
        eprintln!(
            "warning: {bad_unpack} file(s) were unpacked but the hash does not match;\n\
            run `lake build` to rebuild these files (and maybe report this)"
        )
    }
    let unknown_unpack = *unknown_unpack.get_mut();
    if unknown_unpack != 0 {
        eprintln!(
            "note: {unknown_unpack} file(s) had hashes which could not be confirmed;\n\
            `lake build` may be necessary"
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
    let ws = parse_workspace_manifest();
    let mut files = vec![];
    get_files_to_unpack(&ws, true, targets, |mod_, pkg, hash, _| {
        files.push((mod_, pkg, hash))
    });
    let send_commit = || {
        let mut buffer = vec![];
        for (_, _, hash) in &files {
            writeln!(buffer, "{hash:016x}").unwrap()
        }
        let mut easy = curl::easy::Easy::new();
        easy.url(&format!("{CACHE_URL}/c/{hash}?{token}")).unwrap();
        easy.put(true).unwrap();
        let mut list = curl::easy::List::new();
        list.append("x-ms-blob-type: BlockBlob").unwrap();
        if !overwrite {
            list.append("If-None-Match: *").unwrap();
        }
        easy.http_headers(list).unwrap();
        let mut cursor = std::io::Cursor::new(buffer);
        easy.read_function(move |buf| Ok(cursor.read(buf).unwrap()))
            .unwrap();
        easy.perform().unwrap();
    };
    let send_files = || {
        if files.is_empty() {
            println!("No files to upload");
            return;
        }
        println!("Attempting to upload {} file(s)", files.len());
        let (send, recv) = crossbeam_channel::unbounded();
        let poll = std::thread::spawn(move || {
            let mut multi = Multi::new();
            multi.set_max_total_connections(50).unwrap();
            let mut empty = true;
            loop {
                match if empty {
                    recv.recv().map_err(|_| TryRecvError::Disconnected)
                } else {
                    recv.try_recv()
                } {
                    Ok(easy) => {
                        multi.add(easy).unwrap();
                        empty = false;
                    }
                    Err(TryRecvError::Empty) => {
                        if !empty {
                            empty = multi.perform().unwrap() == 0;
                        }
                    }
                    Err(TryRecvError::Disconnected) => {
                        while !empty {
                            empty = multi.perform().unwrap() == 0;
                        }
                        return;
                    }
                }
            }
        });
        (&files).into_par_iter().for_each(|&(ref mod_, pkg, hash)| {
            let tarfile = ltar_path(&cache_dir, hash);
            if overwrite || !tarfile.exists() {
                let tarfile = BufWriter::new(File::create(&tarfile).unwrap());
                let mut paths = vec![
                    mod_.to_path(&pkg.rel_lean_lib_dir, "olean"),
                    mod_.to_path(&pkg.rel_lean_lib_dir, "ilean"),
                    mod_.to_path(&pkg.rel_native_lib_dir, "c"),
                ];
                let extra = mod_.to_path(&pkg.rel_lean_lib_dir, "extra");
                if pkg.build_dir.join(&extra).exists() {
                    paths.push(extra)
                }
                leangz::ltar::pack(
                    &pkg.build_dir,
                    tarfile,
                    mod_.to_path(&pkg.rel_lean_lib_dir, "trace")
                        .to_str()
                        .unwrap(),
                    paths
                        .into_iter()
                        .map(|path| path.to_str().unwrap().to_owned()),
                    false,
                )
                .unwrap();
            }
            let mut easy = curl::easy::Easy::new();
            easy.url(&cache_url(hash, Some(&token))).unwrap();
            easy.put(true).unwrap();
            let mut list = curl::easy::List::new();
            list.append("x-ms-blob-type: BlockBlob").unwrap();
            if !overwrite {
                list.append("If-None-Match: *").unwrap();
            }
            easy.http_headers(list).unwrap();
            let mut tarfile = BufReader::new(File::open(tarfile).unwrap());
            easy.read_function(move |buf| Ok(tarfile.read(buf).unwrap()))
                .unwrap();
            let _ = send.send(easy);
        });
        drop(send);
        poll.join().unwrap();
    };
    rayon::join(send_commit, send_files);
}

fn clean_cache(everything: bool) {
    let cache_dir = cache_dir_from_env();
    let rd = match std::fs::read_dir(cache_dir) {
        Err(e) if e.kind() == ErrorKind::NotFound => return,
        e => e.unwrap(),
    };
    let mut keep = HashSet::new();
    if !everything {
        let ws = parse_workspace_manifest();
        let targets = get_targets(std::iter::empty());
        get_files_to_unpack(&ws, true, targets, |_, _, hash, _| {
            keep.insert(format!("{hash:016x}"));
        });
    }
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
