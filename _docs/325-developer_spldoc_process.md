---
title: "Toolkit SPLDoc process"
permalink: /docs/developer/spldoc_process
excerpt: "How to generate SPL doc for the gh-pages documentation"
last_modified_at: 2018-02-22T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}

The following steps can be followed to generate new SPLDocs for the toolkit and to add it to the github pages. 

**NOTE:** It is recommended that a new repository be cloned in `/tmp` to isolate SPL doc generation and
github pages documentation from development work.

**TODO: The following procedure must be verified!**

1. `cd /tmp`
1. `git clone git@github.com:IBMStreams/streamsx.messagehub.git streamsx.messagehub.gh-pages`
1. `cd streamsx.messagehub.gh-pages`
1. Check out the release, for which you want to generate the SPL documentation, for example `git checkout v1.2.4`
1. `./gradlew spldoc`
1. `git checkout gh-pages`
1. `pushd doc`
1. The SPL documentation for the latest release, which is also available via the SPLDOC button on the entry page, is stored in subdirectory
`spldoc`. Documentation of older releases are stored in versioned sub directories, for example `v1.2.3`. If you update existing documentation,
replace the generated documentation in the right directory.
    * If you update latest SPLDOC: `git rm -r ./spldoc && mv -v ../docs/spldoc .`
    * If you update an older SPLDOC (here v1.2.3): `git rm -r v1.2.3/spldoc && mv -v ../docs/spldoc v1.2.3`
    * If you add SPLDOC of a new version, (here latest version used to be v1.2.4):
       * move current latest doc into versioned directory: `mkdir v1.2.4 && git mv ./spldoc v1.2.4`
       * add generated doc as latest doc: `mv -v ../docs/spldoc .`
1. `popd`
1. remove the generated `docs` directory: `rm -rf docs`
1. Add untracked documentation to the index
1. If you added new documentation, update the `_docs/210-user_spldoc.md` markdown file, which references all SPL docs.
1. `git commit -m "Update docs for vX.Y.Z`
1. `git push -u origin gh-pages`
1. When the result is ok for you, delete the temporary clone that you created in the second step
