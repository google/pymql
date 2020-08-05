# MQL, the Metaweb Query Language

This repository contains the original implementation of the Metaweb Query
Language, written in Python.

## Building / Using MQL

Even though MQL is written in Python, this particular version of it requires
[Bazel](https://bazel.build) to operate properly. You can build the simple
command-line MQL query tool like so:

```
[¬º-°]¬ bazel build :mqlbin
INFO: Analyzed target //:mqlbin (6 packages loaded, 36 targets configured).
INFO: Found 1 target...
Target //:mqlbin up-to-date:
  bazel-out/k8-py2-fastbuild/bin/mqlbin
  INFO: Elapsed time: 0.771s, Critical Path: 0.02s
  INFO: 0 processes.
  INFO: Build completed successfully, 1 total action
```

Then, it can be executed out of the bazel build directory:

*IMPORTANT!* This will only work if you have properly you need to have an
instance of [graphd](https://github.com/google/graphd) running and it needs to
be properly **bootstrapped** for MQL (see below).

```
[¬º-°]¬ bazel-out/k8-py2-fastbuild/bin/mqlbin --graphd_addr=localhost:8100 --mqlcmd=read '{"id": "/type/object/type", "guid": null}'
<logs...>
MQLResult(result={'guid': '#d119a8c0400062d1800000000000000c', 'id': '/type/object/type'}, cost=defaultdict(<type 'float'>, {'pr': 0.0, 'va': 38742.0, 'tu': 22.0, 'in': 3975.0, 'ir': 0.0, 'tr': 23.0, 'ts': 0.0, 'iw': 0.0, 'te': 26.0, 'mql_utime': 0.047658000000000006, 'mql_dbreqs': 11, 'dw': 0.0, 'tg': 0.030711889266967773, 'tf': 0.04290890693664551, 'pf': 0.0, 'mql_rtime': 1.1784470081329346, 'dr': 5619.0, 'gqr': 0, 'mql_stime': 0.0009940000000000018}), dateline=None, cursor=None)

```

## Bootstrapping a graphd for MQL

PyMQL comes with a graphd bootstrap program that you can use to bootstrap an
empty graphd for use with MQL. The bootstrap program itself writes the set of
core types required for MQL to operate.

First, ensure you have a graphd running:

```
[¬º-°]¬ git clone https://github.com/google/graphd
Cloning into 'graphd'...
remote: Enumerating objects: 1259, done.
remote: Total 1259 (delta 0), reused 0 (delta 0), pack-reused 1259
Receiving objects: 100% (1259/1259), 2.57 MiB | 14.95 MiB/s, done.
Resolving deltas: 100% (482/482), done.
[¬º-°]¬ cd graphd
[¬º-°]¬ bazel build graphd
...(graphd builds)
Target //graphd:graphd up-to-date:
  bazel-bin/graphd/graphd
  INFO: Elapsed time: 29.584s, Critical Path: 0.87s
  INFO: 373 processes: 373 linux-sandbox.
  INFO: Build completed successfully, 377 total actions
[¬º-°]¬ bazel-bin/graphd/graphd -d /tmp/data-dir -p /tmp/graphd.pid -n
<graphd is now running in the foreground>
```

In another terminal, run the bootstrap:

```
[¬º-°]¬ ./bazel-out/k8-py2-fastbuild/bin/bootstrap/bootstrap --load bootstrap/otg.bootstrap
```

The bootstrap takes a few minutes to run and you'll see lots of
`graphd.request.start` and `graphd.request.end` lines. This is normal.

After this is done, you can run MQL queries via mqlbin.

## History

This code was originally authored by Tim Sturge, then maintained by Warren
Harris after his departure.

Dime ("2 MQL's") was the implementation written by Warren in OCaml that offered
significant improvements over this initial implementation. However, when Metaweb
was acquired by Google nearing the end of the productionization of Dime, it was
only used partially until Freebase was turned down a few years later. In the
meantime, Warren had gone on to develop other tools used during the early days
of the Knowledge Graph projects at Google.
