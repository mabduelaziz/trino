## My notes toward oss implementation:

##### add GcsFileSystemModule to the following :

1- io.trino.filesystem.manager.FileSystemModule

FileSystemModule is the entry point to add new file system which start with 

```
if (config.isNativeGcsEnabled()) {
install(new GcsFileSystemModule());
factories.addBinding("gs").to(GcsFileSystemFactory.class);
}
```

2- io.trino.spooling.filesystem.FileSystemSpoolingModule
```
if (config.isGcsEnabled()) {
install(new GcsFileSystemModule());
factories.addBinding("gs").to(GcsFileSystemFactory.class);
}
```

