workspace(name = "pymql")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Abseil-py
http_archive(
    name = "absl_py",
    sha256 = "fe3948746ca0543f01fb7767fb00bb739c7fe7e2514180c1575100b988b66542",
    strip_prefix = "abseil-py-master",
    urls = ["https://github.com/abseil/abseil-py/archive/master.zip"],
)

http_archive(
    name = "six_archive",
    build_file = "@//bazel:six.BUILD",
    sha256 = "105f8d68616f8248e24bf0e9372ef04d3cc10104f1980f54d57b2ce73a5ad56a",
    strip_prefix = "six-1.10.0",
    urls = [
        "http://mirror.bazel.build/pypi.python.org/packages/source/s/six/six-1.10.0.tar.gz",
        "https://pypi.python.org/packages/source/s/six/six-1.10.0.tar.gz",
    ],
)
