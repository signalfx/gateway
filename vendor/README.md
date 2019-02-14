# Notes About Dependencies

## etcd

We vendor [etcd](https://github.com/etcd-io/etcd/), but because of the project's structure `govendor` is unable to
fetch all dependencies by specifying a single subpackage like `github.com/coreos/etcd/embed`. It does fetch some
stuff, but it still misses stuff. The work around with `govendor` is to specify `github.com/coreos/etcd/...` which
will vendor all of the needed subpackages in the etcd repo.

```bash
govendor fetch +o github.com/coreos/etcd/...@v3.3.12
```

* The `+o` will fetch unvendored dependencies.

* `etcd` is migrating it's repo name from `github.com/coreos/etcd` to `go.etcd.io/etcd`.  At the moment, the `3.3.x`
line is still being published with the old imports.  This could change in the future and we will need to replace the
coreos imports in the vendor.json.  We will also need to update our imports in this project as well as our fork of
etcd-cloud-operator.

## etcd-cloud-operator

We [forked](https://github.com/signalfx/etcd-cloud-operator) a project called 
[etcd-cloud-operator](https://github.com/Quentin-M/etcd-cloud-operator), but have not changed the import paths with in
it. This means we need do some extra things with `govendor` to checkout our fork in place of the upstream project in
the vendor directory. There are newer and more mature dependency managers out there that can handle this more
gracefully. We also could change the import paths in the future if it is unlikely that our modifications will be
accepted upstream.

```bash
govendor fetch github.com/quentin-m/etcd-cloud-operator/pkg/etcd::github.com/signalfx/etcd-cloud-operator/pkg/etcd@08f0f657908ae53e05d56c29cc3d8dac9d22360b

govendor fetch github.com/quentin-m/etcd-cloud-operator/pkg/providers::github.com/signalfx/etcd-cloud-operator/pkg/providers@08f0f657908ae53e05d56c29cc3d8dac9d22360b

govendor fetch github.com/quentin-m/etcd-cloud-operator/pkg/providers/snapshot::github.com/signalfx/etcd-cloud-operator/pkg/providers/snapshot@08f0f657908ae53e05d56c29cc3d8dac9d22360b

govendor fetch github.com/quentin-m/etcd-cloud-operator/pkg/providers/snapshot/etcd::github.com/signalfx/etcd-cloud-operator/pkg/providers/snapshot/etcd@08f0f657908ae53e05d56c29cc3d8dac9d22360b
```

We have to specify each package when we use `::` with `govendor`.  It is not capable of checking out the whole repo 
in a single statement.
