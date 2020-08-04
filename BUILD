# Author: rtp@google.com (Tyler Pirtle)
#
# Description:
#  mql - implementation(s) of the Metaweb Query Language

package(default_visibility = ["//visibility:public"])

py_library(
    name = "mql",
    srcs = [
        "__init__.py",
        "tid.py",
        "error.py",
        "api/__init__.py",
        "api/envelope.py",
        "formats/__init__.py",
        "formats/http.py",
        "util/__init__.py",
        "util/dumper.py",
        "util/keyquote.py",
        "util/mwdatetime.py",
    ] + glob([
        "log/*.py",
        "mql/*.py",
        "mql/graph/*.py",
    ]),
    deps = [
        "@absl_py//absl:app",
        "@absl_py//absl/flags",
        "@absl_py//absl/logging",
    ],
)

#py_test(
#    name = "pymql_import_test",
#    size = "small",
#    srcs = ["pymql_import_test.py"],
#    deps = [
#        ":mql",
#        "//pyglib",
#        "//testing/pybase",
#    ],
#)

py_binary(
    name = "mqlbin",
    srcs = ["mqlbin.py"],
    python_version = "PY2",
    deps = [
        ":mql",
    ],
)

test_suite(
    name = "AllTests",
    tests = [
        "//third_party/py/pymql/test:AllTests",
    ],
)
