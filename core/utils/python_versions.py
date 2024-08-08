import pkg_resources

installed_packages = [(pkg.key, pkg.version) for pkg in pkg_resources.working_set]
for package, version in installed_packages:
    print(f"{package}=={version}")
