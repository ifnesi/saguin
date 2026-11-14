"""Build an entry point around mqtt_test's unchanged, pinned source modules."""
import json
import pathlib
import re
import shutil
import sys

source, dest = map(lambda p: pathlib.Path(p).resolve(), sys.argv[1:])
dest.mkdir(parents=True, exist_ok=True)
(dest / "src").mkdir(exist_ok=True)
for name in ("Cargo.toml", "Cargo.lock"):
    shutil.copyfile(source / name, dest / name)
modules = sorted((source / "src/tests").glob("*.rs"))
modules = [p for p in modules if p.stem != "mod"]
names = {}
for path in modules:
    code = path.read_text()
    for const in re.findall(r"suite\.add\(\s*(\w+)\s*,", code):
        match = re.search(r"const " + const + r": TestContext = TestContext \{(.*?)\n\};", code, re.S)
        if not match:
            sys.exit("cannot read registered test " + const)
        desc = re.search(r'description: ("(?:[^"\\]|\\.)*"),', match[1])
        refs = re.search(r"refs: &\[(.*?)\]", match[1], re.S)
        if not desc or not refs:
            sys.exit("cannot read test context " + const)
        names[path.stem + "." + const] = {
            "module": path.stem, "description": json.loads(desc[1]),
            "refs": re.findall(r'"([^"]+)"', refs[1]),
        }
if not names:
    sys.exit("mqtt_test registered no tests")
(dest / "names.json").write_text(json.dumps(names, indent=2) + "\n")
lines = []
for name in ("capabilities", "client", "codec", "helpers", "report", "ws"):
    lines.append(f'#[path = {json.dumps(str(source / "src" / (name + ".rs")))}] mod {name};')
lines += ["mod types {", f'include!({json.dumps(str(source / "src/types.rs"))});',
          "impl SuiteRunner<'_> {",
          "    pub fn select(mut self, description: &str) -> Self {",
          "        self.tests.retain(|(ctx, _)| ctx.description == description);",
          "        self", "    }", "}", "}"]
for path in modules:
    lines.append(f'#[path = {json.dumps(str(path))}] mod {path.stem};')
lines += ["fn registry<'a>(name: &str, config: types::TestConfig<'a>) -> types::SuiteRunner<'a> {",
          "    match name {"]
for path in modules:
    lines.append(f'        "{path.stem}" => {path.stem}::tests(config),')
lines += ['        _ => panic!("unknown suite: {name}"),', "    }", "}",
          pathlib.Path("internal/interop/mqtt_test.rs").read_text()]
(dest / "src/main.rs").write_text("\n".join(lines))
print("\n".join(names))
