"""Run upstream unittest unchanged, including its legacy boolean results."""
import ast
import logging
import pathlib
import runpy
import sys
import unittest

source, name, port = sys.argv[1:]
if name == "list":
    tree = ast.parse(pathlib.Path(source).read_text())
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "Test":
            for method in node.body:
                if isinstance(method, ast.FunctionDef) and method.name.startswith("test"):
                    print(method.name)
    sys.exit(0)

# Both scripts initialize their fixtures in __main__. Intercept only the
# unittest entry point, after those fixtures exist, to set the endpoint.
main = unittest.main

def run_tests(*args, **kwargs):
    module = sys.modules["__main__"]
    module.host, module.port = "127.0.0.1", int(port)
    # The suite asks for a forbidden filter; Sagüin reserves $SYS.
    module.nosubscribe_topics = ("$SYS/interop",)
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("mqtt-client").setLevel(logging.DEBUG)
    main(module=module, argv=[source, "Test." + name], verbosity=2)


def call_method(self, method):
    self.assertIsNot(method(), False, "Paho test returned False")

unittest.main = run_tests
unittest.TestCase._callTestMethod = call_method
sys.path.insert(0, str(pathlib.Path(source).resolve().parent))
sys.argv = [source]
runpy.run_path(source, run_name="__main__")
