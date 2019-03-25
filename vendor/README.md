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
