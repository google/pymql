# Author: bneutra@google.com (Brendan Neutra)
#
# Description: mql query language tests.
#

package(default_visibility = ["//visibility:public"])

licenses(["unencumbered"])  # Google acquisition

exports_files(["LICENSE"])

py_library(
    name = "testing_deps",
    testonly = 1,
    srcs = [
        "mql_fixture.py",
    ],
    data = [
        ":config.cfg",
    ] + glob([
        "data/*.yaml",
    ]),
    deps = [
        "//base",
        "//loadbalancer/gslb/client/public:pywrapgslbchannel",
        "//metaweb/graphd/server:graphd_py_pb2",
        "//pyglib",
        "//testing/pybase",
        "//third_party/py/pymql:mql",
        "//third_party/py/simplejson:simplejson_fast",
        "//third_party/py/yaml",
    ],
)

py_test(
    name = "type_link_test",
    size = "large",
    srcs = [
        "type_link_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "cost_test",
    size = "large",
    srcs = [
        "cost_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "sort_test",
    size = "large",
    srcs = [
        "sort_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "basic_mql_test",
    size = "large",
    srcs = [
        "basic_mql_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "mids_test",
    size = "large",
    srcs = [
        "mids_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "best_hrid_test",
    size = "medium",
    srcs = [
        "best_hrid_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "mql_fixture_test",
    size = "large",
    srcs = [
        "mql_fixture_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "regression_id_test",
    size = "large",
    srcs = [
        "regression_id_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "regression_misc_test",
    size = "large",
    srcs = [
        "regression_misc_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "mql_manual_test",
    size = "large",
    srcs = [
        "mql_manual_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "mql_manual_two_test",
    size = "large",
    srcs = [
        "mql_manual_two_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "mql_manual_write_test",
    size = "large",
    srcs = [
        "mql_manual_write_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "return_test",
    size = "large",
    srcs = [
        "return_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "mql_exceptions_test",
    size = "large",
    srcs = [
        "mql_exceptions_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)

py_test(
    name = "query_sort_test",
    size = "small",
    srcs = [
        "query_sort_test.py",
    ],
    deps = [
        ":testing_deps",
    ],
)
