use std::str::Chars;

use crate::{Name, WorkspaceManifest};

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

pub(crate) fn parse_name(it: &mut Chars<'_>) -> Name {
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

fn finish_comment(it: &mut Chars<'_>) {
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
                finish_comment(it)
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

fn parse_imports_core(it: &mut Chars<'_>) -> Vec<(Name, bool)> {
    let mut imports = vec![];
    ws(it);
    if keyword(it, "prelude").is_none() {
        imports.push(("Init".into(), false))
    }
    while keyword(it, "import").is_some() {
        let rt = keyword(it, "runtime").is_some();
        let p = parse_name(it);
        ws(it);
        imports.push((p, rt))
    }
    imports
}

pub(crate) fn parse_imports(s: &str) -> Vec<(Name, bool)> {
    parse_imports_core(&mut s.chars())
}

pub(crate) fn try_parse_lakefile(_s: &str) -> Option<WorkspaceManifest> {
    // let mut it = s.chars();
    // parse_imports_core(&mut it);
    // TODO: lean-lite?
    None
}
